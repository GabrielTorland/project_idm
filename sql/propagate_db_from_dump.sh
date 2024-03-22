nohup docker exec -i mysql sh -c 'exec mysql -uroot -p"Password123"' < all-databases.sql > output_dump.log 2>&1 &

