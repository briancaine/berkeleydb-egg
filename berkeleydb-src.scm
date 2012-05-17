(import chicken scheme)
(import foreign)
(use lolevel ports)

(foreign-declare
  "#include <db.h>
   #include <errno.h>")

(define DB_CREATE (foreign-value "DB_CREATE" int))
(define DB_EXCL (foreign-value "DB_EXCL" int))
(define DB_RDONLY (foreign-value "DB_RDONLY" int))
(define DB_TRUNCATE (foreign-value "DB_TRUNCATE" int))

(define EINVAL (foreign-value "EINVAL" int))

(define +db-pointer-size+
  (max (foreign-value "sizeof(DB**)" int)
       (foreign-value "sizeof(DB*)" int)))

(define DB*-pointer-ref
  (foreign-lambda* c-pointer ((c-pointer x))
    "C_return(x);"))

(define-record berkeley-db ptr path)
(define-record-printer (berkeley-db db port)
  (with-output-to-port port
     (lambda ()
        (printf "#<berkeley-db ~a on ~s>"
                (or (berkeley-db-ptr db) "(closed)")
                (berkeley-db-path db)))))

(define db-create
  (let ((int-db-create
           (foreign-lambda int db_create c-pointer c-pointer unsigned-int)))
    (lambda (#!key dbenv (flags 0))
      (let* ((res-ptr (allocate (* +db-pointer-size+ 2)))
             (result (int-db-create res-ptr dbenv flags))
             (final-ptr (let ((x (DB*-pointer-ref res-ptr)))
                          (free res-ptr)
                          x)))
        (cond
          ((zero? result) (make-berkeley-db final-ptr #f))
          ((equal? EINVAL result) (error "Invalid flag or parameter"))
          (else (error "Unknown error")))))))
