#!/bin/bash

docker build -t csv2pdf ./csv2pdf-docker

docker run --rm -v $(pwd)/output:/output csv2pdf /csv2pdf --landscape --latex_encode --theme Zurich --title 'Stripe transactions' --in /output/Stripe.csv --out /output/Stripe.pdf
docker run --rm -v $(pwd)/output:/output csv2pdf /csv2pdf --landscape --latex_encode --theme Zurich --title 'Paypal transactions' --in /output/Paypal.csv --out /output/Paypal.pdf
