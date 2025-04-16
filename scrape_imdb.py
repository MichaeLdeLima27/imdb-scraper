import requests
import time
import csv
import random
import os
import concurrent.futures
from bs4 import BeautifulSoup

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.246'
}

MAX_THREADS = 10

def write_csv_header():
    if not os.path.exists('movies.csv'):
        with open('movies.csv', mode='w', newline='', encoding='utf-8') as file:
            movie_writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            movie_writer.writerow(['Title', 'Date', 'Rating', 'Plot'])

def extract_movie_details(movie_link):
    try:
        time.sleep(random.uniform(0, 0.2))
        response = requests.get(movie_link, headers=headers, timeout=10)
        response.raise_for_status()
        movie_soup = BeautifulSoup(response.content, 'html.parser')

        title, date, rating, plot_text = None, None, None, None

        page_section = movie_soup.find('section', attrs={'class': 'ipc-page-section'})
        if page_section:
            divs = page_section.find_all('div', recursive=False)
            if len(divs) > 1:
                target_div = divs[1]
                title_tag = target_div.find('h1')
                if title_tag and title_tag.find('span'):
                    title = title_tag.find('span').get_text()

                date_tag = target_div.find('a', href=lambda href: href and 'releaseinfo' in href)
                if date_tag:
                    date = date_tag.get_text().strip()

        rating_tag = movie_soup.find('div', attrs={'data-testid': 'hero-rating-bar__aggregate-rating__score'})
        rating = rating_tag.get_text().strip() if rating_tag else None

        plot_tag = movie_soup.find('span', attrs={'data-testid': 'plot-xs_to_m'})
        plot_text = plot_tag.get_text().strip() if plot_tag else None

        if all([title, date, rating, plot_text]):
            print(title, date, rating, plot_text)
            with open('movies.csv', mode='a', newline='', encoding='utf-8') as file:
                movie_writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                movie_writer.writerow([title, date, rating, plot_text])

    except Exception as e:
        print(f"[ERRO] Falha ao processar {movie_link}: {e}")

def extract_movies(soup):
    try:
        main_column = soup.find('div', attrs={'data-testid': 'chart-layout-main-column'})
        if not main_column:
            print("[ERRO] Não foi possível encontrar a seção principal.")
            return

        movies_table = main_column.find('ul')
        movies_table_rows = movies_table.find_all('li') if movies_table else []

        movie_links = [
            'https://imdb.com' + movie.find('a')['href']
            for movie in movies_table_rows
            if movie.find('a') and '/title/' in movie.find('a')['href']
        ]

        threads = min(MAX_THREADS, len(movie_links))
        with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
            executor.map(extract_movie_details, movie_links)

    except Exception as e:
        print(f"[ERRO] Falha ao extrair os filmes: {e}")

def main():
    start_time = time.time()

    write_csv_header()

    popular_movies_url = 'https://www.imdb.com/chart/moviemeter/?ref_=nv_mv_mpm'
    try:
        response = requests.get(popular_movies_url, headers=headers, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        extract_movies(soup)
    except Exception as e:
        print(f"[ERRO] Não foi possível acessar a página principal: {e}")

    end_time = time.time()
    print('Total time taken: {:.2f} segundos'.format(end_time - start_time))

if __name__ == '__main__':
    main()

# Exercício EBAC - Alteração feita