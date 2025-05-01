# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter

import csv


class MoviesParserPipeline:
    def open_spider(self, spider):
        self.file = open('movies_output.csv', 'w', newline='', encoding='utf-8')
        self.writer = csv.writer(self.file)
        # Укажи нужные заголовки, в зависимости от item
        self.writer.writerow(['title', 'genre', 'director', 'country', 'year', 'imdb'])

    def close_spider(self, spider):
        self.file.close()

    def process_item(self, item, spider):
        # Записывай нужные поля из item
        self.writer.writerow([
            item.get('title'),
            item.get('genre'),
            item.get('director'),
            item.get('country'),
            item.get('year'),
            item.get('imdb')
        ])
        return item
