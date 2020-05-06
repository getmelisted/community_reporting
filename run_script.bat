set script=%1
docker run --rm community python %script%
::set /p id="Enter ID: "