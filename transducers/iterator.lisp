(in-package :transducers)

(defstruct (iter-acc)
  (iter)
  (reducer))

(defstruct (iterator (:constructor %make-iterator)
                     (:print-function (lambda (iterator stream depth)
                                     (declare (ignore depth))
                                     (print-unreadable-object (iterator stream :type t :identity t)
                                       (with-slots (iter reducer) (iterator-acc iterator)
                                         (let ((current (if (eq iter *done*)
                                                            iter
                                                            (car iter))))
                                           (format stream "reduced: ~a, transduced: ~a" reducer current)))))))
  (acc)
  (f)
  (iter))

(defun make-iterator (xform f)
  (lambda (source)
    (let ((iter-f (lambda (&optional (acc nil a-p) (input nil i-p))
                    (cond ((and a-p i-p) (make-iter-acc :reducer (funcall f (iter-acc-reducer acc) input)
                                                        :iter (cl:cons input t)))
                          ((and a-p (not i-p)) (make-iter-acc :reducer (funcall f (iter-acc-reducer acc))
                                                              :iter (cl:cons *done* nil)))
                          (t (make-iter-acc :reducer (funcall f)
                                            :iter (cl:cons 'uninitialized nil)))))))
      (%make-iterator :acc (funcall iter-f)
                      :f (funcall xform iter-f)
                      :iter source))))

(defun next-1 (iterator)
  (with-slots (acc (source-iter iter)) iterator
    (let ((iter-acc (iter-acc-iter acc)))
      ;; When it's already done, just return
      (when (eq (car iter-acc) *done*)
        (return-from next-1 (values acc *done* *done*)))
      ;; First call must initialize the source-iter
      (when (eq (car iter-acc) 'uninitialized)
        (funcall (source-iter-initialize source-iter))))
    (multiple-value-bind (new-acc source-value done) (source-next-1 (iterator-f iterator)
                                                                    acc
                                                                    (source-iter-next source-iter))
      (declare (ignore source-value))
      (when done
        (setf new-acc (funcall (iterator-f iterator) new-acc))
        (funcall (source-iter-finalize source-iter)))
      (setf acc new-acc)
      (values acc (car (iter-acc-iter acc)) done))))

(defun next (iterator)
  (let ((acc-before (iterator-acc iterator)))
    (multiple-value-bind (acc-after source-value done) (next-1 iterator)
      (cond
        ;; Done
        (done (values (iter-acc-reducer acc-after) source-value done))
        ;; Never called the reducer, so the value never got through
        ((eq acc-before acc-after) (next iterator))
        ;; Got value
        ((cdr (iter-acc-iter acc-after)) (values (iter-acc-reducer acc-after) source-value done))
        ;; Didn't get value
        (t (next iterator))))))

(defun iterator-reduce (iterator)
  (multiple-value-bind (acc value done) (next iterator)
    (declare (ignore value))
    (if done
        acc
        (iterator-reduce iterator))))

(defun ensure-source-iter (thing)
  (if (source-iter-p thing)
      thing
      (source->source-iter thing)))

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
