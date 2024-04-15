(in-package :transducers)

(defstruct iter-acc
  (:documentation "Accumulator/state for the `iterator' reducer.

  `i-p' is true if the iterator reducer got an input.

  `input' is one of three possible values; 'UNINITIALIZED if the iterator has not
  yet started, `*DONE*' if the iterator has finished, or the previous input value
  for the iterator reducer if `i-p' is true.

  `acc' is the accumulator for the wrapped reducer.")
  (i-p nil :type boolean)
  (input nil :type t)
  (acc nil :type t))

(defstruct (iterator (:constructor %make-iterator)
                     (:print-function (lambda (iterator stream depth)
                                        (declare (ignore depth))
                                        (print-unreadable-object (iterator stream :type t :identity t)
                                          (with-slots (input acc) (iterator-acc iterator)
                                            (format stream "acc: ~a, input: ~a" acc input))))))
  (:documentation "Iterating over `iter' using `f' as the reducer, keeping state
  in `acc'.")
  (acc nil :type iter-acc)
  (f nil :type (function (&optional iter-acc t) iter-acc))
  (iter nil :type source-iter))

(defun make-iterator (xform f)
  (lambda (source)
    (let ((iter-f (lambda (&optional (acc nil a-p) (input nil i-p))
                    (cond ((and a-p i-p) (make-iter-acc :acc (funcall f (iter-acc-acc acc) input)
                                                        :input input
                                                        :i-p i-p))
                          ((and a-p (not i-p)) (make-iter-acc :acc (funcall f (iter-acc-acc acc))
                                                              :input *done*
                                                              :i-p i-p))
                          (t (make-iter-acc :acc (funcall f)
                                            :input 'uninitialized
                                            :i-p i-p))))))
      (%make-iterator :acc (funcall iter-f)
                      :f (funcall xform iter-f)
                      :iter source))))

(declaim (ftype (function (iterator) (values iter-acc t t)) next-1))
(defun next-1 (iterator)
  (with-slots (acc (source-iter iter)) iterator
    (let ((input (iter-acc-input acc)))
      ;; When it's already done, just return
      (when (eq input *done*)
        (return-from next-1 (values acc *done* *done*)))
      ;; First call must initialize the source-iter
      (when (eq input 'uninitialized)
        (funcall (source-iter-initialize source-iter))))
    (multiple-value-bind (new-acc source-value done) (source-next-1 (iterator-f iterator)
                                                                    acc
                                                                    (source-iter-next source-iter))
      (declare (ignore source-value))
      (when done
        (setf new-acc (funcall (iterator-f iterator) new-acc))
        (funcall (source-iter-finalize source-iter)))
      (setf acc new-acc)
      (values acc (iter-acc-input acc) done))))

(declaim (ftype (function (iterator) (values iter-acc t t)) next))
(defun next (iterator)
  (let ((acc-before (iterator-acc iterator)))
    (multiple-value-bind (acc-after source-value done) (next-1 iterator)
      (cond
        ;; Done
        (done (values (iter-acc-acc acc-after) source-value done))
        ;; Never called the reducer, so the value never got through
        ((eq acc-before acc-after) (next iterator))
        ;; Got value
        ((iter-acc-i-p acc-after) (values (iter-acc-acc acc-after) source-value done))
        ;; Didn't get value
        (t (next iterator))))))

(declaim (ftype (function (iterator) iter-acc) iterator-reduce))
(defun iterator-reduce (iterator)
  (multiple-value-bind (acc value done) (next iterator)
    (declare (ignore value))
    (if done
        acc
        (iterator-reduce iterator))))

(defun multi-iter (source &rest more-sources)
  (let ((sources (mapcar #'ensure-source-iter (cl:cons source more-sources))))
    (make-source-iter
     :initialize (lambda ()
                   (dolist (source sources)
                     (funcall (source-iter-initialize source))))
     :finalize (lambda ()
                 (dolist (source sources)
                   (funcall (source-iter-finalize source))))
     :next (lambda ()
             (block next
               (let ((result '()))
                 (dolist (source sources (nreverse result))
                   (let ((value (funcall (source-iter-next source))))
                     (if (eq value *done*)
                         (return-from next *done*)
                         (push value result))))))))))
