#!/bin/bash

PORT=50000

./client $PORT "PUT:Jitesh:Shah"
./client $PORT "PUT:Jitesh1:Shah1"
echo "Expecting: Shah"
./client $PORT "GET:Jitesh"

./client $PORT "PUT:Salil:Kanitkar"
./client $PORT "PUT:Salil1:Kanitkar1"
echo "Expecting: Kanitkar1"
./client $PORT "GET:Salil1"

./client $PORT "PUT:Mukul:Sinha"
./client $PORT "PUT:Mukul1:Sinha1"
echo "Expecting: Sinha"
./client $PORT "GET:Mukul"

./client $PORT "PUT:a:b"
./client $PORT "PUT:b:c"
./client $PORT "PUT:y:z"
./client $PORT "PUT:Onkar:Dombe"
./client $PORT "GET:Onkar"

echo "Expecting: b"
./client $PORT "GET:a"

echo "Expecting: z"
./client $PORT "GET:y"

