# Setting-up-a-potgres-server
Utils to set up a postgres server with pgvector and populate it using Python and .csv file

Run ```docker compose up -d``` to run the image with both pgadmin and postgres

Then enable pgvector:

1. Run ```docker network create pgnet``` to create a network that will be used by other images to access it.
2. Run ```docker exec -it mascot-postgres psql -U postgres``` to access the image console and run psql.
3. Enable vector extension ```CREATE EXTENSION IF NOT EXISTS vector;```.

