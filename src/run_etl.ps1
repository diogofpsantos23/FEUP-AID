$ErrorActionPreference = "Stop"

docker compose up -d mysql adminer

Get-Content -Raw ".\sql\etl.sql" |
  docker exec -i feup_aid_mysql mysql --local-infile=1 --protocol=tcp -h 127.0.0.1 -uroot -proot dw

Write-Host "ETL finished."
Write-Host "Adminer: http://localhost:8080 (Server=mysql, User=root, Pass=root, DB=dw)"

docker compose run --rm query_runner