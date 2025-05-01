import json
import re
import ast
import scrapy
from collections import deque


class MoviesSpider(scrapy.Spider):
    name = "movies"
    allowed_domains = ["ru.wikipedia.org", "imdb.com"]

    def start_requests(self):
        url = "https://ru.wikipedia.org/wiki/%D0%9A%D0%B0%D1%82%D0%B5%D0%B3%D0%BE%D1%80%D0%B8%D1%8F:%D0%A4%D0%B8%D0%BB%D1%8C%D0%BC%D1%8B_%D0%BF%D0%BE_%D0%B3%D0%BE%D0%B4%D0%B0%D0%BC"
        yield scrapy.Request(url=url, callback=self.parse,
                             headers={'Use-Agent':'Mozilla/5.0'})

        # Используем очередь как в BFS

    visited = set()
    queue = deque()

    def parse(self, response):
        # Собираем все ссылки под заголовками <h3>
        links = response.css('.mw-category-group a::attr(href)').getall()
        for link in links:
            url = response.urljoin(link)
            if url not in self.visited:
                self.queue.append(url)
                self.visited.add(url)

        # Запускаем обход из очереди
        while self.queue:
            next_url = self.queue.popleft()
            yield scrapy.Request(next_url, callback=self.parse_bfs)

    def parse_bfs(self, response):
        # Если есть инфобокс — вызываем парсинг фильма
        if response.css('.infobox'):
            yield from self.parse_movie(response)

        # Ищем следующие ссылки, добавляем в очередь
        new_links = response.css('.mw-category-group a::attr(href)').getall()
        for link in new_links:
            url = response.urljoin(link)
            if url not in self.visited:
                self.queue.append(url)
                self.visited.add(url)

        # Продолжаем обход, если остались ссылки
        if self.queue:
            next_url = self.queue.popleft()
            yield scrapy.Request(next_url, callback=self.parse_bfs)

        next_page_href = response.css('#mw-pages a').xpath('./text()[.="Следующая страница"]/../@href').extract_first()
        if next_page_href:
            yield response.follow(next_page_href, callback=self.parse)
        full_url = response.urljoin(next_page_href)
        print(full_url)

    def clean_title(self, title):
        """Очистить название фильма от лишних кавычек и пробелов"""
        # Убираем лишние кавычки в начале и в конце строки
        title = title.strip().strip('"').strip()

        return title

    def clean_year(self, year_input):
        """Оставить только уникальные года из 4 цифр и вернуть строку, разделенную запятыми"""
        # Преобразуем строку в настоящий список, если это строковое представление списка
        if isinstance(year_input, str):
            try:
                year_input = ast.literal_eval(year_input)
            except (ValueError, SyntaxError):
                pass  # Если строка не является корректным представлением списка, ничего не делаем

        # Теперь год должен быть списком, так что обрабатываем его как список
        if isinstance(year_input, list):
            # Преобразуем все элементы в строки и находим все года
            year_input = ' '.join(map(str, year_input))

        years = re.findall(r'\b\d{4}\b', year_input)  # Ищем все года в строке

        # Убираем дубликаты и сортируем
        unique_years = sorted(set(years), reverse=True)

        # Возвращаем строку годов, разделенную запятыми
        return ', '.join(unique_years)

    def clean_director(self, director_list):
        """Очистить список режиссеров от мусора и оставить только похожее на имена"""
        cleaned_list = []
        for c in director_list:
            c = c.strip()
            if not c:
                continue
            if not c.strip(','):
                continue
            # Убираем лишние пробелы вокруг точек
            c = re.sub(r'\s*\.\s*', '.', c)
            # Убираем лишние пробелы вокруг дефисов
            c = re.sub(r'\s*-\s*', '-', c)

            # Теперь проверяем через регулярку
            if re.fullmatch(r"[A-Za-zА-Яа-яЁё][-A-Za-zА-Яа-яЁё'\.\s]{2,}", c):
                cleaned_list.append(c)

        director = ', '.join(cleaned_list)
        director = re.sub(r'\s*,\s*', ', ', director)
        director = re.sub(r',+', ',', director)
        director = director.strip(' ,')

        return director

    def clean_country(self, country_list):
        """Очистить список стран от мусора"""
        cleaned_list = []
        for c in country_list:
            c = c.strip()
            if not c:
                continue

            # Убираем ссылки/сноски типа [1], [12], [a], просто []
            c = re.sub(r'\[.*?\]', '', c)
            c = c.strip()

            # Убираем одиночные цифры, символы типа "~", мусор
            if re.fullmatch(r'[\d~\-\+]+', c):
                continue

            # Убираем пустые или невалидные записи вроде ",", "[]", и тому подобное
            if re.fullmatch(r'[\[\],\s]*', c):
                continue

            if re.match(r'\s*(ок\.?|ОК|~|≈)', c):
                continue

            # Обрабатываем символ "/" как разделитель стран
            if '/' in c:
                parts = [part.strip() for part in c.split('/') if part.strip()]
                cleaned_list.extend(parts)
            else:
                # Убираем лишние скобки
                c = re.sub(r'^\s*\($', '', c)
                c = re.sub(r'^\s*\)$', '', c)
                if c:
                    cleaned_list.append(c)


        # Склеиваем через запятую
        country = ', '.join(cleaned_list)
        country = re.sub(r'\s*,\s*', ', ', country)
        country = re.sub(r',+', ',', country)
        country = country.strip(' ,')

        return country

    def clean_genre(self, genre_list):
        """Очистить список жанров от мусора"""
        cleaned_list = []

        # Если genre_list это одна строка, разбить её по запятым
        if isinstance(genre_list, str):
            genre_list = [g.strip() for g in genre_list.split(',') if g.strip()]

        for c in genre_list:
            c = c.strip()
            if not c:
                continue
            # Убираем ссылки/сноски типа [1], [2], [] и пр.
            c = re.sub(r'\[.*?\]', '', c).strip()

            # Иногда жанры разделены слэшем
            if '/' in c:
                parts = [part.strip() for part in c.split('/') if part.strip()]
                cleaned_list.extend(parts)
            else:
                if c:
                    cleaned_list.append(c)

        # Удаляем пустые строки ещё раз
        cleaned_list = [g for g in cleaned_list if g]

        final_list = []
        for genre in cleaned_list:
            genre = genre.strip()
            if not genre:
                continue

            # Убираем если много мусорных символов (не буквенных)
            non_letter_ratio = len(re.findall(r'[^A-Za-zА-Яа-яЁё\s-]', genre)) / max(len(genre), 1)
            if non_letter_ratio > 0.3:
                continue

            # Убираем если есть явные мусорные куски
            if any(substr in genre for substr in ['mw-', '{', '}', 'background', 'padding', 'color:', 'output']):
                continue

            # Убираем короткие или цифровые элементы
            if len(genre) >= 3 and not genre.isdigit():
                final_list.append(genre)

        # И ещё раз удаляем пустые
        final_list = [g for g in final_list if g]

        # Склеиваем
        genre = ', '.join(final_list)

        # Убираем подряд идущие запятые
        genre = re.sub(r',\s*,+', ', ', genre)
        genre = genre.strip(' ,')

        return genre

    def parse_movie(self, response):

        infobox = response.css('table.infobox')
        if infobox:
            title_parts = response.css('h1#firstHeading ::text').getall()
            if title_parts:
                title = ''.join(title_parts).strip()
            else:
                title = None


            # Выбираем первый элемент из найденных
            selector = infobox[0]  # берём только первый infobox
            director = []
            genre = []
            country = []
            year = []
            imdb = None

            # Обрабатываем строки таблицы
            for row in selector.css('tr'):
                header = row.css('th::text').extract_first()
                header_genre = row.css('th a::text').extract_first()
                if not header_genre:
                    header_genre = row.css('th::text').extract_first()

                if header and 'Режиссёр' in header:
                    director_list = row.css('td *::text').getall()
                    director_list = [c.strip() for c in director_list if c.strip()]
                    director.extend(director_list)  # Добавляем в список всех режиссёров

                if header_genre and 'Жанр' in header_genre:
                    # Пытаемся вытащить жанры из ссылок
                    genre_list = row.css('td a::text').getall()
                    genre_list = [g.strip() for g in genre_list if g.strip()]

                    # Если в ссылках ничего нет — пробуем весь текст
                    if not genre_list:
                        genre_list = row.css('td *::text').getall()
                        genre_list = [g.strip() for g in genre_list if g.strip()]

                    genre.extend(genre_list)

                if header and ('Страна' in header or 'Страны' in header):
                    # Первый способ: общий сбор текста
                    country_list = row.css('td *::text').getall()
                    country_list = [c.strip() for c in country_list if c.strip()]

                    # Если не сработало — fallback на все span теги
                    if not country_list:
                        country_list = row.css('td span span *::text').getall()
                        country_list = [c.strip() for c in country_list if c.strip()]

                        # Если не сработало — fallback на <a> теги
                        if not country_list:
                            country_list = row.css('td span span *::text').getall()
                            country_list = [c.strip() for c in country_list if c.strip()]

                    country.extend(country_list)

                if header and (
                        'Год' in header or 'Дата Выхода' in header or 'Дата выхода' in header or 'Премьера' in header):
                    year_list = row.css('td *::text').getall()
                    year_list = [c.strip() for c in year_list if c.strip()]
                    year.extend(year_list)  # Добавляем все года

            # Применяем очистку данных
            title = self.clean_title(title)
            director = self.clean_director(director)
            genre = self.clean_genre(genre)
            country = self.clean_country(country)
            year = self.clean_year(year)

            # Ищем ссылку на IMDb

            imdb_url = response.css('th a[title="Internet Movie Database"]')
            if imdb_url:
                imdb_link = response.css('td a::attr(href)').re_first(r'https://www.imdb.com/title/tt\d+/')
                if imdb_link:
                    yield scrapy.Request(
                        url=imdb_link,
                        callback=self.parse_imdb,
                        meta={
                            'wikipedia_data': {
                                'title': title,
                                'genre': genre,
                                'director': director,
                                'country': country,
                                'year': year
                            }
                        },
                        headers={'User-Agent': 'Mozilla/5.0'}
                    )
                else:
                    # Если ссылки нет, просто выдаём данные с Википедии
                    yield {
                        "title": title,
                        "genre": genre,
                        "director": director,
                        "country": country,
                        "year": year,
                        "imdb": None
                    }

            return  # Прерываем дальнейшую обработку для этой страницы


    def parse_imdb(self, response):
        rating = None
        wiki_data = response.meta['wikipedia_data']
        try:
            script_data = response.xpath('//script[@type="application/ld+json"]/text()').get()
            json_data = json.loads(script_data)
            rating = json_data.get('aggregateRating', {}).get('ratingValue')
        except Exception as e:
            self.logger.error(f"Ошибка при извлечении рейтинга: {e}")

        yield {
            "title": wiki_data["title"],
            "genre": wiki_data["genre"],
            "director": wiki_data["director"],
            "country": wiki_data["country"],
            "year": wiki_data["year"],
            "imdb": rating
        }