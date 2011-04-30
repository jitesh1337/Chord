#!/bin/bash

./client 50000 "PUT:Jitesh:Shah"
./client 50000 "PUT:Jitesh1:Shah1"
echo "Expecting: Shah"
./client 50000 "GET:Jitesh"

./client 50000 "PUT:Salil:Kanitkar"
./client 50000 "PUT:Salil1:Kanitkar1"
echo "Expecting: Kanitkar1"
./client 50000 "GET:Salil1"

./client 50000 "PUT:Mukul:Sinha"
./client 50000 "PUT:Mukul1:Sinha1"
echo "Expecting: Sinha"
./client 50000 "GET:Mukul"

./client 50000 "PUT:a:b"
./client 50000 "PUT:b:c"
./client 50000 "PUT:y:z"
./client 50000 "PUT:Onkar:Dombe"
./client 50000 "GET:Onkar"

echo "Expecting: b"
./client 50000 "GET:a"

echo "Expecting: z"
./client 50000 "GET:y"

