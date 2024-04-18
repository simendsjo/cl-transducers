(in-package :transducers)

(defgeneric transduce (xform f source)
  (:documentation "The entry-point for processing some data source via transductions.

This requires three things:

- A transducer function, or a composed chain of them
- A reducing function
- A source

Note: `comp' can be used to chain transducers together.

When ran, `transduce' will pull values from the source, transform them via the
transducers, and reduce into some single value (likely some collection but not
necessarily). `transduce' will only pull as many values from the source as are
actually needed, and does so one at a time. This ensures that large
sources (like files) don't consume too much memory.

# Examples

Assuming that you've required this library with a local nickname of `t', here's
how we can filter an infinite source and reduce into a single sum:

(t:transduce (t:comp (t:filter #'oddp)
                     (t:take 1000)
                     (t:map (lambda (n) (* n n))))
             #'+ (t:ints 1))
;; => 1333333000 (31 bits, #x4F790C08)

Note that due to how transducer and reducer functions are composed internally,
the order provided to `comp' gets applied from top to bottom. In the above
example, this means that `filter' is applied first, and `map' last.

There are a variety of functions to instead reduce into a collection:

(t:transduce (t:map #'1+) #'t:vector '(1 2 3))
;; => #(2 3 4)

Many standard collections can be easily \"sourced\", including those that aren't
normally so conveniently traversed like Hash Tables, Property Lists, and lines
of a file.

;; Read key-value pairs from a plist and recollect into a Hash Table.
(t:transduce #'t:pass #'t:hash-table (t:plist `(:a 1 :b 2 :c 3)))

# Custom Sources

Since `transduce' is generic, you can use `defmethod' to define your own custom
sources. See `sources.lisp' and `entry.lisp' for examples of how to do this.

"))

(defmacro pipe* (fn source &rest transducers-and-reducer)
  (let ((transducers (butlast transducers-and-reducer))
        (reducer (car (cl:last transducers-and-reducer))))
    (if (cdr transducers)
        `(,fn (comp ,@transducers) ,reducer ,source)
        `(,fn ,(car transducers) ,reducer ,source))))

(defmacro pipe (source &rest transducers-and-reducer)
  "Structure `transduce' as a pipeline.

The second up to (but not including) the last argument is used as the
transducers, and the last argument is used as the reducer. If there are more
than one transducer, they are wrapped in `comp'.

(macroexpand-1 '(pipe source t1 reducer))
;; => (TRANSDUCE T1 REDUCER SOURCE), T

(macroexpand-1 '(pipe source t1 t2 reducer))
;; => (TRANSDUCE (COMP T1 T2) REDUCER SOURCE), T
"
  (assert (>= (length transducers-and-reducer) 2) nil
          "Missing transducer or reducer.")
  `(pipe* transduce ,source ,@transducers-and-reducer))

(defmacro iter* (source &rest transducers-and-reducer)
  `(pipe* iterator ,source ,@transducers-and-reducer))

(defmacro iter (source &rest transducers)
  (if transducers
      `(pipe* iterator ,source ,@transducers #'pass-reducer)
      `(pipe* iterator ,source #'pass #'pass-reducer)))

(defgeneric iterator (xform f source))

(defmethod iterator (xform f source)
  (funcall (make-iterator xform f) (if (source-iter-p source)
                                       source
                                       (source->source-iter source))))

#+nil
(pipe '(1 2 3) (take 1) #'*)
#+nil
(pipe (cycle '(1 2 3)) (filter #'oddp) (map #'1+) (take 10) #'*)

(defmacro for ((bindings &rest source-and-transducers) &body body)
  "Execute BODY for each value in the SOURCE-AND-TRANSDUCERS pipeline.

Returns T.

The first value of SOURCE-AND-TRANSDUCERS is treated as the source, and the rest
as a list of transducers. An additional transducer is appended which executes
BODY for each produced value. `for-each' is used as the reducer.

BINDINGS is bound before each execution of BODY. If BINDINGS is a symbol, it is
it is bound by `let', if it's a list, it's bound by `destructuring-bind'.

# Examples

(for (x '(1 2))
  (format t \" ~a\" x))
;; =>  1 2

(for (x '(1 2) (map #'1+))
  (format t \" ~a\" x))
;; =>  1 2

(for ((k v) '((:a 1) (:b 2)))
  (format t \" ~a=~a\" k v))
;; =>  A=1 B=2
"
  (let* ((source (car source-and-transducers))
         (transducers (cdr source-and-transducers))
         (value (gensym "FOR-VALUE-")))
    `(pipe ,source
       ,@transducers
       (map (lambda (,value)
              ,(if (consp bindings)
                   `(destructuring-bind ,bindings ,value
                      ,@body)
                   `(let ((,bindings ,value))
                      ,@body))))
       #'for-each)))

#+nil
(for (x '(1 2))
  (format t "~a~%" x))
#+nil
(for (x (ints 0) (filter #'oddp) (take 2))
  (format t "~a~%" x))
#+nil
(for ((k v) '((:a 1) (:b 2)))
  (format t "~a=~a~%" k v))

(defmethod transduce (xform f (source source-iter))
  (source-iter-transduce xform f source))

(defmethod transduce (xform f (source list))
  "Transducing over an alist works automatically via this method, and the pairs are
streamed as-is as cons cells."
  (source-iter-transduce xform f (list-iter source)))

(defmethod transduce (xform f (source cl:string))
  (source-iter-transduce xform f (string-iter source)))

(defmethod transduce (xform f (source cl:vector))
  (source-iter-transduce xform f (vector-iter source)))

(defmethod transduce (xform f (source pathname))
  "Transduce over the lines of the file named by a FILENAME."
  (source-iter-transduce xform f (file-line-iter source)))

(defmethod transduce (xform f (source stream))
  "Transduce over the lines of a given STREAM. Note: Closing the stream is the
responsiblity of the caller!"
  (source-iter-transduce xform f (stream-line-iter source)))

(defmethod transduce (xform f (source cl:hash-table))
  "Yields key-value pairs as cons cells."
  (hash-table-transduce xform f source))

(defmethod transduce (xform f (source plist))
  "Yields key-value pairs as cons cells.

# Conditions

- `imbalanced-pist': if the number of keys and values do not match."
  (source-iter-transduce xform f (plist-iter (plist-list source))))

(defmethod transduce (xform f source)
  "Fallback for types which don't implement this. Always errors.

# Conditions

- `no-transduce-implementation': an unsupported type was transduced over."
  (source-iter-transduce xform f (source->source-iter source)))

(defstruct source-iter (:documentation "An iterator over a source of values.

`source-iter-next' should return the next element in the source, or `*done*'
when finished.  After `source-iter-next' returns `*done*' it should continue to
return `*done*' for each subsequent call without doing any work or fail. Each
value, other than `*done*' must be returned once and only once in the same order
as in the source.

`source-iter-initialize' must be called before `source-iter-next' is called the
first time. This should do required setup of the source, e.g. opening a file.
The function should be idempontent to avoid problems if called more than once.

`source-iter-finalize' must be called after `source-iter-next' returns `*done*'
the first time, or when choosing to abort iteration. This should clean up any
resources allocated by `source-iter-initialize'. The function should be
idempotent to avoid problems if called more than once. `source-iter-next' must
not be called after finalizing.")
  (next (lambda () *done*) :type (function () t))
  (initialize (lambda ()) :type (function () t))
  (finalize (lambda ()) :type (function () t)))

(declaim (ftype (function (list) source-iter) list-iter))
(defun list-iter (list)
  (let ((rest list))
    (make-source-iter :next (lambda ()
                              (if (null rest)
                                  *done*
                                  (pop rest))))))

(declaim (ftype (function (list) source-iter) plist-iter))
(defun plist-iter (lst)
  (let ((items lst))
    (make-source-iter
     :next (lambda ()
             (cond ((null items) *done*)
                   ((null (cdr items))
                    (let ((key (pop items)))
                      (restart-case (error 'imbalanced-plist :key key)
                        (use-value (value)
                          :report "Supply a value for the final key."
                          :interactive (lambda () (prompt-new-value (format nil "Value for key ~a: " key)))
                          (list key value)))))
                   (t (cl:cons (pop items) (pop items))))))))

(declaim (ftype (function (cl:vector) source-iter) vector-iter))
(defun vector-iter (vector)
  (let ((len (length vector))
        (i 0))
    (make-source-iter :next (lambda ()
                              (if (eql i len)
                                  *done*
                                  (prog1 (aref vector i)
                                    (incf i)))))))

(declaim (ftype (function (cl:string) source-iter) string-iter))
(defun string-iter (string)
  (vector-iter string))

(declaim (ftype (function (pathname) source-iter) file-line-iter))
(defun file-line-iter (pathname)
  (let ((file nil))
    (make-source-iter :next (lambda ()
                              (or (read-line file nil) *done*))
                      :initialize (lambda ()
                                    (unless file
                                      (setf file (open pathname))))
                      :finalize (lambda ()
                                  (when (and file (open-stream-p file))
                                    (close file)
                                    (setf file nil))))))

(declaim (ftype (function (stream) source-iter) stream-line-iter))
(defun stream-line-iter (stream)
  (make-source-iter :next (lambda ()
                            (or (read-line stream nil) *done*))))

(declaim (ftype (function (generator) source-iter) generator-iter))
(defun generator-iter (generator)
  (make-source-iter :next (lambda ()
                            (funcall (generator-func generator)))))

(defgeneric source->source-iter (source)
  (:documentation "Constructing a `source-iter' from SOURCE."))

(declaim (ftype (function (t) source-iter) ensure-source-iter))
(defun ensure-source-iter (thing)
  "Calls `source->source-iter' iff THING is not `source-iter-p'"
  (if (source-iter-p thing)
      thing
      (values (source->source-iter thing))))

(defmethod source->source-iter (fallback)
  (error 'no-transduce-implementation :type (type-of fallback)))

(defmethod source->source-iter ((source list))
  (list-iter source))

(defmethod source->source-iter ((source cl:vector))
  (vector-iter source))

(defmethod source->source-iter ((source pathname))
  (file-line-iter source))

(defmethod source->source-iter ((source stream))
  (stream-line-iter source))

(defmethod source->source-iter ((source generator))
  (generator-iter source))

(declaim (ftype (function ((function (t t) t) t (function () t)) (values t t t)) source-next-1))
(defun source-next-1 (f acc next)
  "Fetch and process the next value, i.e. (F ACC (NEXT))

NEXT should be a function as described by `source-iter-next'.
F is a \"reducer\" function for ACC. It can return `reduced' to tell that the
iteration is done.

Returns three values:
1. The result of calling the reducer, F on ACC and the next item. Only called
when the next item is not `*done*'.
2. The item produced by NEXT (see `source-iter-next' for semantics).
3. `*done*' if no item was produced or F decided to stop."
  (let* ((item (funcall next))
         (done (eq item *done*))
         (new-acc (if done
                      acc
                      (funcall f acc item))))
    (if (reduced-p new-acc)
        (values (reduced-val new-acc) item *done*)
        (values new-acc item (if done *done* nil)))))

(declaim (ftype (function (t t source-iter) *) source-iter-transduce))
(defun source-iter-transduce (xform f source)
  (let* ((init (funcall f))
         (xf (funcall xform f))
         (result (source-iter-reduce xf init source)))
    (funcall xf result)))

(defun source-iter-reduce (xf init source)
  (prog2
      (funcall (source-iter-initialize source))
      (unwind-protect
           (labels ((recurse (acc)
                      (multiple-value-bind (new-acc item done) (source-next-1 xf acc (source-iter-next source))
                        (declare (ignore item))
                        (if done
                            new-acc
                            (recurse new-acc)))))
             (recurse init))
        (funcall (source-iter-finalize source)))))

(declaim (ftype (function (t t cl:hash-table) *) hash-table-transduce))
(defun hash-table-transduce (xform f coll)
  "Transduce over the contents of a given Hash Table."
  (let* ((init   (funcall f))
         (xf     (funcall xform f))
         (result (hash-table-reduce xf init coll)))
    (funcall xf result)))

(defun hash-table-reduce (f identity ht)
  (with-hash-table-iterator (iter ht)
    (labels ((recurse (acc)
               (multiple-value-bind (entry-p key value) (iter)
                 (if (not entry-p)
                     acc
                     (let ((acc (funcall f acc (cl:cons key value))))
                       (if (reduced-p acc)
                           (reduced-val acc)
                           (recurse acc)))))))
      (recurse identity))))

#+nil
(transduce (map #'1+) #'cons #(1 2 3 4 5))
#+nil
(transduce #'pass #'cons (plist `(:a 1 :b 2 :c 3)))
#+nil
(transduce (map #'car) #'cons (plist `(:a 1 :b 2 :c 3)))
#+nil
(transduce (map #'cdr) #'+ (plist `(:a 1 :b 2 :c)))  ;; Imbalanced plist for testing.
#+nil
(transduce #'pass #'cons '((:a . 1) (:b . 2) (:c . 3)))
#+nil
(transduce (map #'char-upcase) #'string "hello")
#+nil
(transduce (map #'1+) #'vector '(1 2 3 4))
#+nil
(transduce (map #'1+) #'+ #(1 2 3 4))
#+nil
(let ((hm (make-hash-table :test #'equal)))
  (setf (gethash 'a hm) 1)
  (setf (gethash 'b hm) 2)
  (setf (gethash 'c hm) 3)
  (transduce (filter #'evenp) (max 0) hm))
#+nil
(transduce (map #'1+) #'+ 1)  ;; Expected to fail.
#+nil
(transduce #'pass #'count #p"/home/colin/history.txt")

(declaim (ftype (function (source-iter &rest source-iter) source-iter) multi-iter))
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
