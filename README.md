# TP Escalabilidad: Coffee Shop Analysis

Igual que en el TP0

`
docker-compose-up
`

`
docker-compose-down
`

`
docker-compose-logs
`

Generar automaticamente un archivo docker-compose

`
./generar-compose.sh <output_file> <number_of_result_nodes> <number_of_select_nodes> <number_of_clients> <number_of_dispatchers>
`

Correr tests unitarios del MessageMiddleware

`
make run-unit-tests
`

Instalar linter y formatter

`
sudo apt install flake8 black isort mypy
`

Linter & format

`
lint_and_format.sh
`

Comparar resultados obtenidos con los resultados esperados

`
make compare-results <expected_results_directory_path> <obtained_results_directory_path>
`