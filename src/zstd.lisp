;;; This file is part of cl-zstd
;;; Copyright 2020-2022 Guillaume LE VAILLANT
;;; Distributed under the GNU GPL v3 or later.
;;; See the file LICENSE for terms of use and distribution.

(in-package :zstd)


(deftype u8 () '(unsigned-byte 8))
(defconstant +buffer-size+ 4096)


;;;
;;; Errors
;;;

(define-condition zstd-error (simple-error)
  ())

(defmacro zstd-error (message &rest args)
  `(error 'zstd-error
          :format-control ,message
          :format-arguments (list ,@args)))

(defmacro zstd-check (form)
  (let ((code (gensym)))
    `(let ((,code ,form))
       (declare (type u64 ,code))
       (if (= (zstd-is-error ,code) 1)
           (zstd-error (zstd-get-error-name ,code))
           ,code))))


;;;
;;; Buffers passed to libzstd
;;;

;;; libzstd's streaming functions take a ZSTD_inBuffer {src, size, pos} and a
;;; ZSTD_outBuffer {dst, size, pos}, and advance POS as they consume input and
;;; produce output.  SRC and DST point at the data of Lisp vectors, and such an
;;; address is valid only while the vector is pinned: a moving garbage
;;; collector may relocate the vector between two calls.  So the structs are
;;; never kept around.  They are built on the stack for each call, while both
;;; vectors are pinned, and POS is copied back to Lisp when the call returns.
;;; This is safe because cl-zstd doesn't enable ZSTD_c_stableInBuffer or
;;; ZSTD_d_stableOutBuffer, so libzstd keeps no reference to these buffers
;;; between calls.

(defmacro with-zstd-buffers (((in-buffer input-buffer input-size input-pos)
                              (out-buffer output-buffer output-pos))
                             &body body)
  "Evaluate BODY with IN-BUFFER bound to a foreign ZSTD_inBuffer describing
the bytes of INPUT-BUFFER between INPUT-POS and INPUT-SIZE, and OUT-BUFFER
bound to a foreign ZSTD_outBuffer describing the free space of OUTPUT-BUFFER
after OUTPUT-POS.  Both vectors are pinned during BODY.  When BODY returns,
the places INPUT-POS and OUTPUT-POS are set to the positions libzstd left in
the structs.  Return the values of BODY."
  (let ((input (gensym "INPUT"))
        (output (gensym "OUTPUT"))
        (src (gensym "SRC"))
        (dst (gensym "DST")))
    `(let ((,input ,input-buffer)
           (,output ,output-buffer))
       (cffi:with-foreign-objects ((,in-buffer '(:struct zstd-in-buffer))
                                   (,out-buffer '(:struct zstd-out-buffer)))
         (cffi:with-pointer-to-vector-data (,src ,input)
           (cffi:with-pointer-to-vector-data (,dst ,output)
             (cffi:with-foreign-slots ((src size pos) ,in-buffer
                                       (:struct zstd-in-buffer))
               (setf src ,src
                     size ,input-size
                     pos ,input-pos))
             (cffi:with-foreign-slots ((dst size pos) ,out-buffer
                                       (:struct zstd-out-buffer))
               (setf dst ,dst
                     size (length ,output)
                     pos ,output-pos))
             (multiple-value-prog1 (progn ,@body)
               (setf ,input-pos (cffi:foreign-slot-value
                                 ,in-buffer '(:struct zstd-in-buffer) 'pos)
                     ,output-pos (cffi:foreign-slot-value
                                  ,out-buffer '(:struct zstd-out-buffer)
                                  'pos)))))))))


;;;
;;; Compression Gray streams
;;;

(defclass compressing-stream (fundamental-binary-output-stream)
  ((output-stream :accessor output-stream)
   (zstd-context :accessor zstd-context)
   (input-buffer :accessor input-buffer)
   (input-size :accessor input-size
               :documentation "Number of bytes waiting in INPUT-BUFFER.")
   (output-buffer :accessor output-buffer)))

(defmethod stream-element-type ((stream compressing-stream))
  '(unsigned-byte 8))

(defun compress-and-write (stream)
  (with-slots (output-stream zstd-context input-buffer input-size output-buffer)
      stream
    (let ((input-pos 0)
          (output-pos 0))
      (with-zstd-buffers ((in-buffer input-buffer input-size input-pos)
                          (out-buffer output-buffer output-pos))
        (zstd-check (zstd-compress-stream2 zstd-context
                                           out-buffer
                                           in-buffer
                                           :zstd-e-continue)))
      (when (plusp input-pos)
        (replace input-buffer input-buffer :start2 input-pos :end2 input-size)
        (decf input-size input-pos))
      (when (plusp output-pos)
        (write-sequence output-buffer output-stream :end output-pos)))))

(defmethod stream-write-byte ((stream compressing-stream) byte)
  (with-slots (input-buffer input-size) stream
    (setf (aref input-buffer input-size) byte)
    (incf input-size))
  (compress-and-write stream)
  byte)

(defmethod stream-write-sequence ((stream compressing-stream) seq start end
                                  &key &allow-other-keys)
  (with-slots (input-buffer input-size) stream
    (loop :while (< start end) :do
      (let* ((available-space (- (length input-buffer) input-size))
             (n (min (- end start) available-space)))
        (replace input-buffer seq
                 :start1 input-size :start2 start :end2 (+ start n))
        (incf input-size n)
        (incf start n))
      (compress-and-write stream)))
  seq)

(defmethod stream-finish-output ((stream compressing-stream))
  (with-slots (output-stream zstd-context input-buffer input-size output-buffer)
      stream
    (let ((input-pos 0))
      (do ((frame-complete-p nil))
          (frame-complete-p)
        (let ((output-pos 0))
          (setf frame-complete-p
                (with-zstd-buffers ((in-buffer input-buffer input-size
                                               input-pos)
                                    (out-buffer output-buffer output-pos))
                  (zerop (zstd-check (zstd-compress-stream2 zstd-context
                                                            out-buffer
                                                            in-buffer
                                                            :zstd-e-end)))))
          (when (plusp output-pos)
            (write-sequence output-buffer output-stream :end output-pos)))))
    (setf input-size 0)
    (finish-output output-stream))
  nil)

(defmethod close ((stream compressing-stream) &key &allow-other-keys)
  (when (open-stream-p stream)
    (finish-output stream)
    (with-slots (zstd-context input-buffer output-buffer) stream
      (zstd-check (zstd-free-cctx zstd-context))
      (setf zstd-context nil)
      (setf input-buffer nil)
      (setf output-buffer nil)))
  t)

(defun initialize-context (context level)
  "Initialize the CONTEXT for the given compression LEVEL."
  (zstd-check (zstd-cctx-set-parameter context :zstd-c-compression-level level))
  (zstd-check (zstd-cctx-set-parameter context :zstd-c-checksum-flag 1))
  context)

(defun make-compressing-stream (output-stream &key (level 3))
  "Return a stream that will compress the bytes written to it at the given
compression LEVEL and write them to the OUTPUT-STREAM."
  (let ((stream (make-instance 'compressing-stream))
        (min-level (zstd-min-c-level))
        (max-level (zstd-max-c-level)))
    (setf (output-stream stream) output-stream)
    (with-slots (zstd-context input-buffer input-size output-buffer) stream
      (if (and (integerp level) (<= min-level level max-level))
          (let ((context (zstd-create-cctx)))
            (if (cffi:null-pointer-p context)
                (zstd-error "Failed to create compression context.")
                (setf zstd-context (initialize-context context level))))
          (zstd-error "LEVEL must be between ~d and ~d." min-level max-level))
      (setf input-buffer (cffi:make-shareable-byte-vector
                          (zstd-c-stream-in-size)))
      (setf input-size 0)
      (setf output-buffer (cffi:make-shareable-byte-vector
                           (zstd-c-stream-out-size))))
    stream))

(defmacro with-compressing-stream ((stream output-stream &key (level 3))
                                   &body body)
  "Within BODY, STREAM is bound to a compressing stream for the given
compression LEVEL and OUTPUT-STREAM. The result of the last form of BODY is
returned."
  `(with-open-stream (,stream (make-compressing-stream ,output-stream
                                                       :level ,level))
     ,@body))


;;;
;;; Decompression Gray streams
;;;

(defclass decompressing-stream (fundamental-binary-input-stream)
  ((input-stream :accessor input-stream)
   (zstd-context :accessor zstd-context)
   (input-buffer :accessor input-buffer)
   (input-size :accessor input-size
               :documentation "Number of bytes waiting in INPUT-BUFFER.")
   (output-buffer :accessor output-buffer)
   (output-size :accessor output-size
                :documentation "Number of decompressed bytes waiting in
OUTPUT-BUFFER.")
   (frame-complete-p :accessor frame-complete-p)))

(defmethod stream-element-type ((stream decompressing-stream))
  '(unsigned-byte 8))

(defun read-and-decompress (stream)
  (with-slots (input-stream zstd-context input-buffer input-size
               output-buffer output-size frame-complete-p)
      stream
    (setf input-size (read-sequence input-buffer input-stream
                                    :start input-size))
    (let ((end-of-input-p (zerop input-size)))
      (unless end-of-input-p
        (let ((input-pos 0))
          (setf frame-complete-p
                (with-zstd-buffers ((in-buffer input-buffer input-size
                                               input-pos)
                                    (out-buffer output-buffer output-size))
                  (zerop (zstd-check (zstd-decompress-stream zstd-context
                                                             out-buffer
                                                             in-buffer)))))
          (when (plusp input-pos)
            (replace input-buffer input-buffer
                     :start2 input-pos :end2 input-size)
            (decf input-size input-pos))))
      end-of-input-p)))

(defmethod stream-listen ((stream decompressing-stream))
  (with-slots (input-stream input-size output-size) stream
    (or (plusp output-size)
        (plusp input-size)
        (listen input-stream))))

(defmethod stream-read-byte ((stream decompressing-stream))
  (with-slots (output-buffer output-size) stream
    (let ((end-of-input-p (read-and-decompress stream)))
      (cond
        ((plusp output-size)
         (let ((byte (aref output-buffer 0)))
           (replace output-buffer output-buffer :start2 1 :end2 output-size)
           (decf output-size)
           byte))
        ((and end-of-input-p (not (frame-complete-p stream)))
         (zstd-error "Truncated stream."))
        (t
         :eof)))))

(defmethod stream-read-sequence ((stream decompressing-stream) seq start end
                                 &key &allow-other-keys)
  (with-slots (output-buffer output-size) stream
    (let ((end-of-input-p nil))
      (loop :until (or (= start end) end-of-input-p) :do
        (setf end-of-input-p (read-and-decompress stream))
        (loop :while (and (< start end) (plusp output-size)) :do
          (let ((n (min (- end start) output-size)))
            (replace seq output-buffer :start1 start :end2 n)
            (replace output-buffer output-buffer :start2 n :end2 output-size)
            (decf output-size n)
            (incf start n))))
      (when (and end-of-input-p (not (frame-complete-p stream)))
        (zstd-error "Truncated stream."))))
  start)

(defmethod close ((stream decompressing-stream) &key &allow-other-keys)
  (when (open-stream-p stream)
    (with-slots (zstd-context input-buffer output-buffer) stream
      (zstd-check (zstd-free-dctx zstd-context))
      (setf zstd-context nil)
      (setf input-buffer nil)
      (setf output-buffer nil)))
  t)

(defun make-decompressing-stream (input-stream)
  "Return a stream that will supply the bytes resulting from the decompression
of the data read from the INPUT-STREAM."
  (let ((stream (make-instance 'decompressing-stream)))
    (setf (input-stream stream) input-stream)
    (with-slots (zstd-context input-buffer input-size
                 output-buffer output-size frame-complete-p)
        stream
      (let ((context (zstd-create-dctx)))
        (if (cffi:null-pointer-p context)
            (zstd-error "Failed to create decompression context.")
            (setf zstd-context context)))
      (setf input-buffer (cffi:make-shareable-byte-vector
                          (zstd-d-stream-in-size)))
      (setf input-size 0)
      (setf output-buffer (cffi:make-shareable-byte-vector
                           (zstd-d-stream-out-size)))
      (setf output-size 0)
      (setf frame-complete-p t))
    stream))

(defmacro with-decompressing-stream ((stream input-stream) &body body)
  "Within BODY, STREAM is bound to a decompressing stream for the given
INPUT-STREAM. The result of the last form of BODY is returned."
  `(with-open-stream (,stream (make-decompressing-stream ,input-stream))
     ,@body))


;;;
;;; Compression functions
;;;

(defun compress-stream (input output &key (level 3))
  "Read the data from the INPUT octet stream, compress it, and write the result
to the OUTPUT octet stream."
  (with-compressing-stream (stream output :level level)
    (let ((buffer (make-array +buffer-size+ :element-type 'u8)))
      (do ((n (read-sequence buffer input) (read-sequence buffer input)))
          ((zerop n) t)
        (write-sequence buffer stream :end n)))))

(defun compress-file (input output &key (level 3))
  "Read the data from the INPUT file, compress it, and write the result to the
OUTPUT file."
  (with-open-file (input-stream input :element-type 'u8)
    (with-open-file (output-stream output :direction :output :element-type 'u8)
      (compress-stream input-stream output-stream :level level))))

(defun compress-buffer (buffer &key (start 0) end (level 3))
  "Read the data between the START and END offsets in the BUFFER, compress it,
and return the resulting octet vector."
  (let ((end (or end (length buffer))))
    (octet-streams:with-octet-output-stream (output)
      (with-compressing-stream (stream output :level level)
        (write-sequence buffer stream :start start :end end)))))


;;;
;;; Decompression functions
;;;

(defun decompress-stream (input output)
  "Read the data from the INPUT octet stream, decompress it, and write the
result to the OUTPUT octet stream."
  (with-decompressing-stream (stream input)
    (let ((buffer (make-array +buffer-size+ :element-type 'u8)))
      (do ((n (read-sequence buffer stream) (read-sequence buffer stream)))
          ((zerop n) t)
        (write-sequence buffer output :end n)))))

(defun decompress-file (input output)
  "Read the data from the INPUT file, decompress it, and write the result to
the OUTPUT file."
  (with-open-file (input-stream input :element-type 'u8)
    (with-open-file (output-stream output :direction :output :element-type 'u8)
      (decompress-stream input-stream output-stream))))

(defun decompress-buffer (buffer &key (start 0) end)
  "Read the data between the START and END offsets in the BUFFER, decompress
it, and return the resulting octet vector."
  (let ((end (or end (length buffer))))
    (octet-streams:with-octet-output-stream (output)
      (octet-streams:with-octet-input-stream (input buffer start end)
        (decompress-stream input output)))))
