mc alias set myminio http://localhost:9000 admin password
mc mb myminio/warehouse
mc policy set private myminio/warehouse