# Scrapy_parser
Scrapy parser

## Goals

Scrapy parser allows you to parse movies from wikipedia. 
Start url: https://ru.wikipedia.org/wiki/%D0%9A%D0%B0%D1%82%D0%B5%D0%B3%D0%BE%D1%80%D0%B8%D1%8F:%D0%A4%D0%B8%D0%BB%D1%8C%D0%BC%D1%8B_%D0%BF%D0%BE_%D0%B3%D0%BE%D0%B4%D0%B0%D0%BC 

### Installation

#### Prerequisites

- python3

#### How to install

- Clone this repository and `cd` into it
  
- Crete a virtual environment
`python -m venv venv`


### Usage

- activate project virtual environment
`.\venv\Scripts\activate`

- install scapy
`pip install scrapy`

- Run `exit` to deactivate environment after work is done

- `cd` into spiders directory
  `cd .\movies_parser\movies_parser\spiders`

- run spider
  `scrapy runspider movies.py`

 - check up an output file movie_parser.csv  
  `

#### Fields

- `title`, - movie title
- `genre`, - movie genre
- `director`, - movie director
- `country`, - movie country
- `year`, - movie year
- `imbd`, - imbd rating
