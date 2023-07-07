import os
import time

for file in os.listdir('books_url'):
    path = os.path.join('books_url', file)
    if file == '.DS_Store':
        continue
    with open(path, 'r', encoding='utf-8') as urls:
        new_urls = []
        for url in urls:
            url = url.replace("https://www.amazon.com/", "https://www.amazon.de/")
            url = url.replace("-/en/", "")
            if url.find("/dp/") == -1:
                continue
            elif url.count("https://www.amazon.de") == 2:
                new_urls.append(url[:url.rfind("https://")]+'\n')
                new_urls.append(url[url.rfind("https://"):])
            else:
                new_urls.append(url)
    data = ''.join(str(element) for element in new_urls)
    with open(path, 'w', encoding='utf-8') as new_file:
        new_file.write(data)

