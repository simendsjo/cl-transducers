(defpackage tra
  (:use :cl)
  (:local-nicknames (#:q #:sycamore))
  (:shadow #:map #:remove #:concatenate #:log
           #:cons #:count))

(in-package :tra)

;; TODO
;; tappend-map
;; tdelete-*

;; --- Transducers --- ;;

(defun map (f)
  "Map an F across all elements of the transduction."
  (lambda (reducer)
    (lambda (&optional (result nil r-p) (input nil i-p))
      (cond ((and r-p i-p) (funcall reducer result (funcall f input)))
            ((and r-p (not i-p)) (funcall reducer result))
            (t (funcall reducer))))))

(defun filter (pred)
  "Only keep elements from the transduction that satisfy PRED."
  (lambda (reducer)
    (lambda (&optional (result nil r-p) (input nil i-p))
      (cond ((and r-p i-p)
             (if (funcall pred input)
                 (funcall reducer result input)
                 result))
            ((and r-p (not i-p)) (funcall reducer result))
            (t (funcall reducer))))))

(defun remove (pred)
  "Remove elements from the transduction that satisfy PRED."
  (lambda (reducer)
    (lambda (&optional (result nil r-p) (input nil i-p))
      (cond ((and r-p i-p)
             (if (not (funcall pred input))
                 (funcall reducer result input)
                 result))
            ((and r-p (not i-p)) (funcall reducer result))
            (t (funcall reducer))))))

(defun filter-map (f)
  "Map a function F over the elements of the transduction, but only keep results
that are non-nil."
  (lambda (reducer)
    (lambda (&optional (result nil r-p) (input nil i-p))
      (cond ((and r-p i-p)
             (let ((x (funcall f input)))
               (if x
                   (funcall reducer result x)
                   result)))
            ((and r-p (not i-p)) (funcall reducer result))
            (t (funcall reducer))))))

;; (list-transduce (filter-map #'first) #'cons '(() (2 3) () (5 6) () (8 9)))

(defun drop (n)
  "Drop the first N elements of the transduction."
  (lambda (reducer)
    (let ((new-n (1+ n)))
      (lambda (&optional (result nil r-p) (input nil i-p))
        (cond ((and r-p i-p)
               (setf new-n (1- new-n))
               (if (> new-n 0)
                   result
                   (funcall reducer result input)))
              ((and r-p (not i-p)) (funcall reducer result))
              (t (funcall reducer)))))))

(defun drop-while (pred)
  "Drop elements from the front of the transduction that satisfy PRED."
  (lambda (reducer)
    (let ((drop? t))
      (lambda (&optional (result nil r-p) (input nil i-p))
        (cond ((and r-p i-p) (if (and (funcall pred input) drop?)
                                 result
                                 (progn (setf drop? nil)
                                        (funcall reducer result input))))
              ((and r-p (not i-p)) (funcall reducer result))
              (t (funcall reducer)))))))

(defun take (n)
  "Keep the first N elements of the transduction."
  (lambda (reducer)
    (let ((new-n n))
      (lambda (&optional (result nil r-p) (input nil i-p))
        (cond ((and r-p i-p)
               (let ((result (if (> new-n 0)
                                 (funcall reducer result input)
                                 result)))
                 (setf new-n (1- new-n))
                 (if (not (> new-n 0))
                     (ensure-reduced result)
                     result)))
              ((and r-p (not i-p)) (funcall reducer result))
              (t (funcall reducer)))))))

(defun take-while (pred)
  "Keep only elements which satisfy a given PRED, and stop the transduction as
soon as any element fails the test."
  (lambda (reducer)
    (lambda (&optional (result nil r-p) (input nil i-p))
      (cond ((and r-p i-p)
             (if (not (funcall pred input))
                 (ensure-reduced result)
                 (funcall reducer result input)))
            ((and r-p (not i-p)) (funcall reducer result))
            (t (funcall reducer))))))

;; (list-transduce (take-while #'evenp) #'cons '(2 4 6 8 9 2))

(defun concatenate (reducer)
  "Concatenates all the sublists in the transduction."
  (let ((preserving-reducer (preserving-reduced reducer)))
    (lambda (&optional (result nil r-p) (input nil i-p))
      (cond ((and r-p i-p) (list-reduce preserving-reducer result input))
            ((and r-p (not i-p)) (funcall reducer result))
            (t (funcall reducer))))))

;; (list-transduce #'concatenate #'cons '((1 2 3) (4 5 6) (7 8 9)))

(defun flatten (reducer)
  "Entirely flattens any list passed through it."
  (lambda (&optional (result nil r-p) (input nil i-p))
    (cond ((and r-p i-p)
           (if (listp input)
               (list-reduce (preserving-reduced (flatten reducer)) result input)
               (funcall reducer result input)))
          ((and r-p (not i-p)) (funcall reducer result))
          (t '()))))

;; (list-transduce #'flatten #'cons '((1 2 3) 0 (4 (5) 6) 0 (7 8 9) 0))

(defun interpolate (elem)
  "Insert an ELEM between each value of the transduction."
  (lambda (reducer)
    (let ((send-elem? nil))
      (lambda (&optional (result nil r-p) (input nil i-p))
        (cond ((and r-p i-p)
               (if send-elem?
                   (let ((result (funcall reducer result elem)))
                     (if (reduced-p result)
                         result
                         (funcall reducer result input)))
                   (progn (setf send-elem? t)
                          (funcall reducer result input))))
              ((and r-p (not i-p)) (funcall reducer result))
              (t (funcall reducer)))))))

;; (list-transduce (interpolate 0) #'cons '(1 2 3))

(defun enumerate (reducer)
  "Index every value passed through the transduction into a cons pair. Starts at 0."
  (let ((n 0))
    (lambda (&optional (result nil r-p) (input nil i-p))
      (cond ((and r-p i-p)
             (let ((input (cl:cons n input)))
               (setf n (1+ n))
               (funcall reducer result input)))
            ((and r-p (not i-p) (funcall reducer result)))
            (t (funcall reducer))))))

;; (list-transduce #'enumerate #'cons '("a" "b" "c"))

(defun log (logger)
  "Call some LOGGER for each step of the transduction. The LOGGER must accept the
running results and the current element as input."
  (lambda (reducer)
    (lambda (&optional (result nil r-p) (input nil i-p))
      (cond ((and r-p i-p)
             (funcall logger result input)
             (funcall reducer result input))
            ((and r-p (not i-p)) (funcall reducer result))
            (t (funcall reducer))))))

;; (list-transduce (log (lambda (_ n) (format t "Got: ~a~%" n))) #'cons '(1 2 3 4 5))

(defun window (n)
  "Yield N-length windows of overlapping values. This is different from `segment' which
yields non-overlapping windows. If there were fewer items in the input than N,
then this yields nothing."
  (unless (and (integerp n) (> n 0))
    (error "The arguments to window must be a positive integer."))
  (lambda (reducer)
    (let ((i 0)
          (q (q:make-amortized-queue)))
      (lambda (&optional (result nil r-p) (input nil i-p))
        (cond ((and r-p i-p)
               (setf q (q:amortized-enqueue q input))
               (setf i (1+ i))
               (cond ((< i n) result)
                     ((= i n) (funcall reducer result (q:amortized-queue-list q)))
                     (t (setf q (q:amortized-dequeue q))
                        (funcall reducer result (q:amortized-queue-list q)))))
              ((and r-p (not i-p)) (funcall reducer result))
              (t (funcall reducer)))))))

;; (list-transduce (window 3) #'cons '(1 2 3 4 5 6 7))

;; --- Reducers --- ;;

(defun cons (&optional (acc nil a-p) (input nil i-p))
  "A transducer-friendly consing reducer with '() as the identity."
  (cond ((and a-p i-p) (cl:cons input acc))
        ((and a-p (not i-p)) (reverse acc))
        (t '())))

(defun count (&optional (acc nil a-p) (input nil i-p))
  "A counting reducer that counts any elements that made it through the
transduction."
  (declare (ignore input))
  (cond ((and a-p i-p) (1+ acc))
        ((and a-p (not i-p)) acc)
        (t 0)))

;; (list-transduce (map #'identity) #'count '(1 2 3 4 5))

(defun any (pred)
  (lambda (&optional (acc nil a-p) (input nil i-p))
    (cond ((and a-p i-p)
           (let ((test (funcall pred input)))
             (if test
                 (make-reduced :val test)
                 nil)))
          ((and a-p (not i-p)) acc)
          (t nil))))

;; (list-transduce (map #'identity) (any #'evenp) '(1 3 5 7 9))
;; (list-transduce (map #'identity) (any #'evenp) '(1 3 5 7 9 2))

(defun all (pred)
  (lambda (&optional (acc nil a-p) (input nil i-p))
    (cond ((and a-p i-p)
           (let ((test (funcall pred input)))
             (if (and acc test)
                 test
                 (make-reduced :val nil))))
          ((and a-p (not i-p)) acc)
          (t t))))

;; (list-transduce (map #'identity) (all #'oddp) '(1 3 5 7 9))
;; (list-transduce (map #'identity) (all #'oddp) '(1 3 5 7 9 2))

;; --- Entry Points --- ;;

;; TODO Provide a single `transduce' function that checks the type of its input
;; and dispatches based on that? I think this is what Clojure does.
(defun list-transduce (xform f coll)
  (list-transduce-work xform f (funcall f) coll))

(defun list-transduce-work (xform f init coll)
  (let* ((xf (funcall xform f))
         (result (list-reduce xf init coll)))
    (funcall xf result)))

(defun list-reduce (f identity lst)
  (if (null lst)
      identity
      (let ((v (funcall f identity (car lst))))
        (if (reduced-p v)
            (reduced-val v)
            (list-reduce f v (cdr lst))))))

;; --- Other Utilities --- ;;

(defstruct reduced
  "A wrapper that signals that reduction has completed."
  val)

(defun ensure-reduced (x)
  "Ensure that X is reduced."
  (if (reduced-p x)
      x
      (make-reduced :val x)))

(defun preserving-reduced (reducer)
 "A helper function that wraps a reduced value twice since reducing
functions (like list-reduce) unwraps them. tconcatenate is a good example: it
re-uses its reducer on its input using list-reduce. If that reduction finishes
early and returns a reduced value, list-reduce would 'unreduce' that value and
try to continue the transducing process."
  (lambda (a b)
    (let ((result (funcall reducer a b)))
      (if (reduced-p result)
          (make-reduced :val result)
          result))))

;; --- Testing --- ;;

;; (defun do-it (items)
;;   ;; (declare (optimize (speed 3) (safety 0)))
;;   (list-transduce (alexandria:compose
;;                    #'enumerate
;;                    (map (lambda (pair) (* (car pair) (cdr pair))))
;;                    (filter #'evenp)
;;                    (drop 3)
;;                    (take 3))
;;                   #'cons
;;                   items))

;; (do-it '(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15))

;; (list-transduce (ttake 3) #'cons '(2 4 6 8 9 1 2))
;; (list-transduce #'concatenate #'cons '((1 2 3) (4 5 6) (7 8 9)))
;; (list-transduce #'flatten #'cons '((1 2 3) 1 8 (4 5 6) (7 8 9) 0))
