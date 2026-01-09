import scrapy
from urllib.parse import urljoin, urlparse
import re
from datetime import datetime, timedelta

class BeltaSpider(scrapy.Spider):
    name = 'belta_presidential_appointments'
    allowed_domains = ['belta.by']

    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'DOWNLOAD_TIMEOUT': 30,
        'DOWNLOAD_DELAY': 2,
        'CONCURRENT_REQUESTS': 1,
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    }

    def __init__(self, *args, **kwargs):
        super(BeltaSpider, self).__init__(*args, **kwargs)
        # Get today's date
        self.today = datetime.now()
        self.logger.info(f"Today's date: {self.today.strftime('%d.%m.%Y')}")

        # Categories that indicate presidential/personnel news
        self.RELEVANT_CATEGORIES = [
            'президент', 'политика', 'власть', 'кадровые решения',
            'назначения', 'указ', 'государство'
        ]

        # Keywords and phrases indicating appointments
        self.APPOINTMENT_PATTERNS = [
            'президент назначил',
            'указом президента назначен',
            'президент принял кадровые решения',
            'назначен на должность',
            'освобожден от должности',
            'освобождён от должности',
            'назначена на должность',
            'назначен',
            'назначена',
            'назначены',
            'присвоено звание',
            'присвоен класс',
            'назначение',
            'кадровые решения',
            'перемещение',
            'увольнение',
            'отставка'
        ]

        # Patterns for extracting person's name (FIO)
        # Cyrillic names: Lastname Firstname Middlename or Firstname Middlename Lastname
        self.FIO_PATTERN = r'([А-ЯЁ][а-яё]+(?:\s+[А-ЯЁ][а-яё]+){1,2})'

        # Patterns for position titles
        self.POSITION_PATTERNS = [
            r'на должность\s+([^.]+)',
            r'назначен\s+([^.]+)',
            r'назначена\s+([^.]+)',
            r'–\s+([^.]+?\s+(?:председатель|начальник|министр|руководитель|директор|заместитель|советник|помощник|представитель)[^.]+)',
            r'—\s+([^.]+?\s+(?:председатель|начальник|министр|руководитель|директор|заместитель|советник|помощник|представитель)[^.]+)',
        ]

    def start_requests(self):
        """Start crawling from the all_news page"""
        start_url = "https://belta.by/all_news/"
        yield scrapy.Request(
            url=start_url,
            callback=self.parse_news_list,
            meta={'page': 1, 'continue_loading': True}
        )

    def parse_news_list(self, response):
        """Parse the news list page and extract article links"""
        page = response.meta['page']
        continue_loading = response.meta.get('continue_loading', True)

        self.logger.info(f"Parsing news list - page {page}")

        # Track if we've found articles from previous day
        found_previous_day = False
        processed_count = 0

        # Extract news items from the list
        news_items = response.css('.lenta_item')

        for item in news_items:
            # Extract date from the item first
            item_date = self.extract_item_date(item)

            # Check if we've reached the previous day - stop loading
            if item_date and item_date.date() < self.today.date():
                self.logger.info(f"Reached previous day: {item_date.strftime('%d.%m.%Y')}, stopping pagination")
                found_previous_day = True
                break

            # Extract title and link
            title_span = item.css('.lenta_item_title')
            if not title_span:
                continue

            title = title_span.css('::text').get()
            if not title:
                # Try to get all text including from nested elements
                title = ' '.join(title_span.css('::text').getall()).strip()

            if not title:
                continue

            # Extract the link
            link = item.css('a::attr(href)').get()
            if link:
                article_url = self.clean_url(link)
            else:
                continue

            # Extract description/snippet
            snippet_span = item.css('.lenta_textsmall')
            snippet = ""
            if snippet_span:
                snippet = ' '.join(snippet_span.css('::text').getall()).strip()

            # Combine title and snippet for analysis
            full_text = f"{title} {snippet}".lower()

            # Check if this is relevant news (presidential appointments)
            if self.is_presidential_appointment(full_text):
                self.logger.info(f"Found presidential appointment: {title}")

                yield scrapy.Request(
                    url=article_url,
                    callback=self.parse_article,
                    meta={
                        'title': title,
                        'snippet': snippet,
                        'item_date': item_date
                    }
                )
                processed_count += 1

        self.logger.info(f"Processed {processed_count} appointments on page {page}")

        # Continue loading if we haven't reached the previous day and continuation is enabled
        if continue_loading and not found_previous_day:
            # Look for "Load more" button with the specific structure
            load_more_divs = response.css('div.load_more')

            if load_more_divs:
                # Extract the onclick attribute and parse the URL
                for div in load_more_divs:
                    onclick = div.css('::attr(onclick)').get()
                    if onclick and 'get_page(' in onclick:
                        # Extract URL from onclick="return get_page('/all_news/page/7/?day=08&amp;month=01&amp;year=26/','inner','1');"
                        url_match = re.search(r"get_page\('([^']+)'", onclick)
                        if url_match:
                            next_page_path = url_match.group(1)
                            # Unescape HTML entities
                            next_page_path = next_page_path.replace('&amp;', '&')

                            # Build full URL
                            next_url = urljoin('https://belta.by', next_page_path)

                            self.logger.info(f"Loading next page: {next_url}")

                            yield scrapy.Request(
                                url=next_url,
                                callback=self.parse_news_list,
                                meta={'page': page + 1, 'continue_loading': True}
                            )
                            break
            else:
                self.logger.info("No more 'Load more' buttons found")

    def parse_article(self, response):
        """Parse individual article to extract appointment details with pipeline logic"""
        title = response.meta['title']
        snippet = response.meta['snippet']
        item_date = response.meta['item_date']

        # Step 1: Extract date from article page using .date_full
        # <div class="date_full">08 января 2026, 19:51</div>
        pub_date = self.extract_article_date(response)

        # Check if article is from today
        is_today = self.check_is_today(pub_date)

        # Step 1: Check title for relevance (today + relevant)
        title_relevant = self.is_presidential_appointment(title)

        if is_today and title_relevant:
            # Pipeline step 1: Title meets conditions (today + relevant)
            relevant = True
            self.logger.info(f"Article '{title}' marked as RELEVANT (title check, today: {is_today})")
        else:
            # Step 2: Check description/article body for relevance
            article_content = self.extract_article_content(response)
            description_relevant = self.is_presidential_appointment(article_content)

            if description_relevant:
                # Pipeline step 4: Description is relevant
                relevant = True
                self.logger.info(f"Article '{title}' marked as RELEVANT (description check)")
            else:
                # Pipeline step 3: Not relevant
                relevant = False
                self.logger.info(f"Article '{title}' marked as NOT RELEVANT")

        # Extract additional details only if relevant (for efficiency)
        if relevant:
            # Combine all text for analysis
            full_text = f"{title} {snippet} {article_content if 'article_content' in locals() else ''}"

            # Extract person's name (FIO)
            fio = self.extract_fio(full_text, title)

            # Extract position
            position = self.extract_position(full_text, title)

            # Create the notification
            notification = self.create_notification(fio, position, response.url)
        else:
            fio = None
            position = None
            notification = None

        yield {
            'title': title,
            'url': response.url,
            'snippet': snippet,
            'fio': fio,
            'position': position,
            'publication_date': pub_date.isoformat() if pub_date else None,
            'is_today': is_today,
            'relevant': relevant,
            'notification_header': notification['header'] if notification else None,
            'notification_text': notification['text'] if notification else None,
            'notification_source': notification['source'] if notification else None,
            'scraping_timestamp': datetime.now().isoformat(),
            'raw_content': article_content[:1000] if 'article_content' in locals() and article_content else ''
        }

    def is_presidential_appointment(self, text):
        """Check if text is about presidential appointment"""
        text_lower = text.lower()

        # Check if it's in relevant categories
        has_category = any(category in text_lower for category in self.RELEVANT_CATEGORIES)

        # Check for appointment patterns
        has_appointment = any(pattern in text_lower for pattern in self.APPOINTMENT_PATTERNS)

        return has_category and has_appointment

    def extract_item_date(self, item):
        """Extract date from news list item using the specific HTML structure:
        <div class="new_date">
            <div class="day">09</div>
            <div class="month_year"><span>.</span>01.26</div>
        </div>
        """
        date_div = item.css('.new_date')
        if date_div:
            day = date_div.css('.day::text').get()
            month_year = date_div.css('.month_year::text').get()

            if day and month_year:
                # Clean up the extracted text
                day = day.strip()
                month_year = month_year.strip().replace('.', '').strip()

                # month_year format is "01.26" (MM.YY)
                # day format is "09"
                try:
                    parts = month_year.split('.')
                    if len(parts) == 2:
                        month = int(parts[0])
                        year_short = int(parts[1])

                        # Convert 2-digit year to 4-digit year
                        # Assuming years 00-49 are 2000-2049, 50-99 are 1950-1999
                        if year_short >= 50:
                            year = 1900 + year_short
                        else:
                            year = 2000 + year_short

                        day_int = int(day)

                        # Create datetime object
                        return datetime(year, month, day_int)
                except (ValueError, IndexError) as e:
                    self.logger.warning(f"Failed to parse date: day={day}, month_year={month_year}, error={e}")

        return None

    def extract_fio(self, full_text, title):
        """Extract person's full name (FIO) from text"""
        # Try to extract FIO from title first
        fio_matches = re.findall(self.FIO_PATTERN, full_text)

        if fio_matches:
            # Filter out common non-person words
            excluded_words = [
                'Президент', 'Республики', 'Беларусь', 'Комитет', 'Совет',
                'Министерство', 'Государственный', 'Национальный', 'Администрация'
            ]

            for match in fio_matches:
                words = match.split()
                # Valid FIO should have 2-3 words
                if 2 <= len(words) <= 3:
                    # Check that it's not an excluded word
                    if not any(excluded in match for excluded in excluded_words):
                        return match.strip()

        return "Не удалось определить ФИО"

    def extract_position(self, full_text, title):
        """Extract position/title from text"""
        # Try various position patterns
        for pattern in self.POSITION_PATTERNS:
            matches = re.findall(pattern, full_text, re.IGNORECASE)
            if matches:
                position = matches[0].strip()
                # Clean up the position string
                position = re.sub(r'\s+', ' ', position)
                position = position.rstrip('.,;:')
                if len(position) > 5:  # Minimum length for position title
                    return position

        # Fallback: look for common position keywords
        position_keywords = [
            'председатель', 'министр', 'начальник', 'директор',
            'руководитель', 'заместитель', 'советник', 'помощник',
            'представитель', 'посол', 'судья', 'прокурор'
        ]

        for keyword in position_keywords:
            # Look for sentences containing these keywords
            pattern = rf'([^.]*{keyword}[^.]*)'
            matches = re.findall(pattern, full_text, re.IGNORECASE)
            if matches:
                return matches[0].strip()

        return "Должность не указана"

    def extract_article_content(self, response):
        """Extract main article content from div[itemprop="articleBody"]"""
        # First try the specific selector: <div itemprop="articleBody">
        article_body = response.css('div[itemprop="articleBody"]')
        if article_body:
            # Get all text content including nested divs and p tags
            # Use ::text on all descendant elements
            all_text = article_body.css('::text').getall()
            if all_text:
                # Join and clean up
                content = ' '.join(text.strip() for text in all_text if text.strip())
                return content

        # Fallback to other selectors
        content_selectors = [
            '.article-text',
            '.news-text',
            '.content',
            'article',
            '.post-content',
            '#article-content',
            '.detail-text',
            '.text'
        ]

        for selector in content_selectors:
            content = response.css(selector)
            if content:
                paragraphs = content.css('p::text').getall()
                if paragraphs:
                    return ' '.join(paragraphs).strip()

        # Fallback: get all p tags
        paragraphs = response.css('p::text').getall()
        return ' '.join(paragraphs).strip() if paragraphs else ""

    def extract_article_date(self, response):
        """Extract publication date from article page using .date_full"""
        # First try the specific selector: <div class="date_full">08 января 2026, 19:51</div>
        date_full = response.css('.date_full::text').get()
        if date_full:
            parsed_date = self.parse_date_full_format(date_full.strip())
            if parsed_date:
                return parsed_date

        # Fallback to other selectors
        date_selectors = [
            'time::attr(datetime)',
            '.date::text',
            '.news-date::text',
            '.publication-date::text',
            'meta[property="article:published_time"]::attr(content)',
            '.meta-date::text',
            '.article-date::text'
        ]

        for selector in date_selectors:
            date_text = response.css(selector).get()
            if date_text:
                parsed_date = self.parse_date_string(date_text.strip())
                if parsed_date:
                    return parsed_date

        return datetime.now()  # Fallback to current time

    def check_is_today(self, pub_date):
        """Check if publication date is today"""
        if not pub_date:
            return False

        return pub_date.date() == self.today.date()

    def parse_date_full_format(self, date_string):
        """Parse date_full format: '08 января 2026, 19:51'"""
        # Russian month names
        ru_months = {
            'января': 1, 'февраля': 2, 'марта': 3, 'апреля': 4,
            'мая': 5, 'июня': 6, 'июля': 7, 'августа': 8,
            'сентября': 9, 'октября': 10, 'ноября': 11, 'декабря': 12
        }

        try:
            # Format: "08 января 2026, 19:51"
            # Remove any extra whitespace
            date_string = re.sub(r'\s+', ' ', date_string).strip()

            # Extract day, month, year, time
            match = re.search(r'(\d{1,2})\s+([а-яё]+)\s+(\d{4})(?:,\s+(\d{1,2}):(\d{2}))?', date_string.lower())
            if match:
                day = int(match.group(1))
                month_ru = match.group(2)
                year = int(match.group(3))

                # Time is optional
                if match.group(4) and match.group(5):
                    hour = int(match.group(4))
                    minute = int(match.group(5))
                    if month_ru in ru_months:
                        return datetime(year, ru_months[month_ru], day, hour, minute)
                else:
                    if month_ru in ru_months:
                        return datetime(year, ru_months[month_ru], day)
        except Exception as e:
            self.logger.warning(f"Failed to parse date_full format '{date_string}': {e}")

        return None

    def parse_date_string(self, date_string):
        """Parse various date formats"""
        if not date_string:
            return None

        date_string = date_string.strip()

        # Common formats to try
        date_formats = [
            '%Y-%m-%dT%H:%M:%S',  # ISO format
            '%Y-%m-%d %H:%M:%S',   # Datetime
            '%d.%m.%Y %H:%M',      # DD.MM.YYYY HH:MM
            '%d.%m.%Y',            # DD.MM.YYYY
            '%d %m %Y',            # DD Month YYYY
        ]

        for fmt in date_formats:
            try:
                return datetime.strptime(date_string, fmt)
            except ValueError:
                continue

        # Try Russian month names
        ru_months = {
            'января': 1, 'февраля': 2, 'марта': 3, 'апреля': 4,
            'мая': 5, 'июня': 6, 'июля': 7, 'августа': 8,
            'сентября': 9, 'октября': 10, 'ноября': 11, 'декабря': 12
        }

        match = re.search(r'(\d{1,2})\s+([а-яё]+)\s+(\d{4})', date_string.lower())
        if match:
            day, month, year = match.groups()
            if month in ru_months:
                try:
                    return datetime(int(year), ru_months[month], int(day))
                except ValueError:
                    pass

        return None

    def create_notification(self, fio, position, url):
        """Create notification with specified format"""
        header = "Новое назначение Президента Республики Беларусь"
        text = f"Опубликовано новое кадровое решение: {fio}, назначен(а) на должность {position}. Необходимо подготовить поздравление."

        return {
            'header': header,
            'text': text,
            'source': url
        }

    def clean_url(self, url):
        """Clean and normalize URL"""
        if not url:
            return None

        # Remove any HTML entities or encoded characters
        url = re.sub(r'&amp;', '&', url)

        # Handle relative URLs
        if url.startswith('//'):
            url = 'https:' + url
        elif url.startswith('/'):
            url = urljoin('https://belta.by', url)
        elif not url.startswith('http'):
            url = urljoin('https://belta.by', url)

        return url
