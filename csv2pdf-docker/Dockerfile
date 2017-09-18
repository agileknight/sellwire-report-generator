FROM ubuntu:16.04

RUN apt-get update && apt-get install -y \
    perl \
    build-essential \
    cpanminus \
    texlive-xetex \
    liblatex-driver-perl \
  && rm -rf /var/lib/apt/lists/*

ADD https://raw.githubusercontent.com/lima1/p5-latex-table/master/bin/csv2pdf /csv2pdf

RUN chmod +x /csv2pdf

RUN cpanm Text::CSV
RUN cpanm Template
RUN cpanm LaTeX::Table
RUN cpanm LaTeX::Encode
