(module berkeleydb
  (
   db-create berkeley-db-ptr berkeley-db-path
   db-open! db-put! db-get db-delete!

   db-cursor db-cursor-get/next

   DB_CREATE

   DB_BTREE DB_HEAP DB_HASH DB_QUEUE DB_RECNO DB_UNKNOWN
  )
  "berkeleydb-src.scm")
