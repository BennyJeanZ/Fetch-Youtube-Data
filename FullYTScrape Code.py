import time
import timeit
import csv
import requests
import multiprocessing
import re
from datetime import date
from collections import deque
from bs4 import BeautifulSoup
from selenium import webdriver
from functools import partial
from selenium.webdriver.common.keys import Keys

# University Channel Page
counter_list = []
homepage = 'https://www.youtube.com'

# Setup Chrome display
options = webdriver.ChromeOptions()
options.add_argument('--ignore-certificate-errors')
options.add_argument("--test-type")

class Methods:
    # Check if the variable datatype is None
    def CheckNone(link):
        if link is not None:
            return True
        else:
            return False

    # Check if link is Unique
    def Unique(link):
        with open('E:/Scrape/Youtube/CollectLinks Code/TestLinks.csv', 'rt', encoding='utf-8') as Linklist:
            reader = csv.reader(Linklist)
            for url in reader:
                if Methods.CheckNone(link) & Methods.CheckNone(url):
                    if link == str(url).replace("['", '').replace("']", ''):
                        Linklist.close()
                        return False
                else:
                    Linklist.close()
                    return False
            return True

# Convert Malay Date into English Date format
def convertDate(word):
    mlist = {'Jan': ['Januari'],
             'Feb': ['Februari'],
             'Mar': ['Mac'],
             'Apr': ['Apr'],
             'May': ['Mei'],
             'Jun': ['Jun'],
             'Jul': ['Julai'],
             'Aug': ['Ogos', 'Ogo'],
             'Sep': ['September'],
             'Oct': ['Okt'],
             'Nov': ['Nov'],
             'Dec': ['Dis']}
    lock = 0
    for key, value in mlist.items():
        for each in value:
            for wd in word.split(" "):
                if each.lower() == wd.lower():
                    word = word.replace(each, key)
                    lock = 1
                    break
            if lock == 1:
                break
        if lock == 1:
            break
    return word.replace('[', ''). replace(']', '').replace("'", '')

def multi_pool(func, url, filename, procs):             # Defines method to handle multiprocessing of collect_data()
    templist = []                                       # Stores the data to be returned from this method.
    counter = len(url)                                  # Number counter for total links left.
    pool = multiprocessing.Pool(processes=procs)
    print('Total number of processes: ' + str(procs))
    part = partial(func, filename)                      # Partial function to accept method with multiple arguments.
    for a in pool.imap(part, url):                      # Loop each collect_data() execution.
        templist.append(a)                              # Puts the details row from collect_data() inside templist
        if '00' in str(counter - len(templist)):        # Shows processing progress only on every hundredth entry
            print('Number of links left: ' + str(counter - len(templist)))
    pool.terminate()
    pool.join()
    return templist

def collect_links(link, country):

    container = ['', '']
    singlevid = 0
    # Use Web driver to scroll down the page first. Do not use --headless options as it doesnt work with scrolling.
    driver = webdriver.Chrome(chrome_options=options, executable_path=r'E:\Scrape\chromedriver.exe')
    print('Opening ' + str(link))
    driver.get(str(link))

    # Scrolling the page once.
    time.sleep(1)
    driver.find_element_by_tag_name('body').send_keys(Keys.END)

    # Condition for link being a single youtube video instead of playlist or channel list.
    if 'watch' in link:
        singlevid = 1

    # Uses a loop condition where it compares the 'before scroll' page height and 'after scrolled' page height.
    # Youtube uses floating web elements and so "return document.body.scrollHeight" does not work.
    if singlevid == 0:
        while True:
            height = driver.execute_script("return document.documentElement.scrollHeight")
            time.sleep(1)
            driver.find_element_by_tag_name('body').send_keys(Keys.END)
            print('Scrolling down the page to load more videos...')
            # print('Ori Height:' + str(height))
            new_height = driver.execute_script("return document.documentElement.scrollHeight")
            # print('New Height:' + str(new_height))
            if int(new_height) == height:
                print('Bottom of the page reached.')
                print('Collecting all video links now.')
                break

    # Collects every single video link in the channel page
    yt_list = []
    with open('E:/Scrape/Youtube/FullTest/YoutubeVideoLinks.csv', 'at', encoding='utf-8', newline='') as Linklist:
        writer = csv.writer(Linklist)
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        if singlevid == 0:
            if 'playlist' in driver.current_url:                    # Gets video links from Youtube playlist page.
                if soup.find_all('ytd-playlist-video-renderer') is not None:
                    navigation = soup.find_all('ytd-playlist-video-renderer')
                    for link in navigation:
                        vidid = link.find('a').get('href')
                        ytlink = homepage + vidid
                        if Methods.Unique(ytlink):  # Check the link to make sure that it is correct and unique
                            u = (str(ytlink).split('\n'))
                            container[0] = str(u).replace("[", "").replace("]", "").replace("'", "")
                            container[1] = country
                            writer.writerow(container)
                        yt_list.append(u)
                        counter_list.append(u)
                    Linklist.close()
            else:                                                   # Gets video links from Channel upload page.
                if soup.find_all('ytd-grid-video-renderer') is not None:
                    navigation = soup.find_all('ytd-grid-video-renderer')
                    for link in navigation:
                        vidid = link.find('a').get('href')
                        ytlink = homepage + vidid
                        if Methods.Unique(ytlink):                  # Check the link to make sure that it is correct and unique
                            u = (str(ytlink).split('\n'))
                            container[0] = str(u).replace("[", "").replace("]", "").replace("'", "")
                            container[1] = country
                            writer.writerow(container)
                        yt_list.append(u)
                        counter_list.append(u)
                    Linklist.close()
                else:
                    navigation = soup.find_all('tr', {'class': 'pl-video'})
                    for link in navigation:
                        vidid = link.get('data-video-id')
                        ytlink = 'https://www.youtube.com/watch?v=' + vidid
                        if Methods.Unique(ytlink):  # Check the link to make sure that it is correct and unique
                            u = (str(ytlink).split('\n'))
                            container[0] = str(u).replace("[", "").replace("]", "").replace("'", "")
                            container[1] = country
                            writer.writerow(container)
                        yt_list.append(u)
                        counter_list.append(u)
                    Linklist.close()
        else:                                                       # collects page link itself as it is a video page.
            ytlink = driver.current_url
            vidid = ytlink.replace(homepage, '')
            if Methods.Unique(ytlink):  # Check the link to make sure that it is correct and unique
                u = (str(ytlink).split('\n'))
                container[0] = str(u).replace("[", "").replace("]", "").replace("'", "")
                container[1] = country
                writer.writerow(container)
            yt_list.append(u)
            counter_list.append(u)
            Linklist.close()

    print('Number of videos in this channel: ' + str(len(yt_list)))
    print('Total number of videos collected: ' + str(len(counter_list)))
    driver.quit()

def collect_data(filename, youtube_url_list):

    finished = 0
    retry_count = 0
    while True:
        while finished != 1:
            try:
                if retry_count == 5:  # If except: No connection, up to 5 times then loop will break
                    print('This link cannot be opened ' + str(youtube_url_list))
                    break
                else:
                    time.sleep(1)  # buffer period
                    print('OPENING ' + str(youtube_url_list))
                    response = requests.get(str(youtube_url_list)).text
                    if response:
                        finished = 1
            except requests.exceptions.ConnectionError as e:  # Multi Pool does not work without defining this
                print(e)
                print('No connection, Retrying...' + youtube_url_list)
                retry_count += 1
                continue
            except requests.exceptions.ReadTimeout as e:
                print(e)
                print('Timeout Error.  ' + youtube_url_list)
                retry_count += 1
                continue

        soup = BeautifulSoup(response, "lxml")
        details = ['', '', '', '', '', '', '']

        # Channel Name / University Channel name
        if soup.find('div', {'class': 'yt-user-info'}) is not None:
            c_name = soup.find('div', {'class': 'yt-user-info'}).get_text()
            details[0] = " ".join(c_name.split())
        else:
            print('Did not find channel name element')
        if details[0] == '':
            details[0] = 'null'

        # Video Title
        if soup.find('title') is not None:
            title = soup.find('title').get_text()
            details[1] = " ".join(title.split())
        else:
            print('Did not find element title')
        if details[1] == '':
            details[1] = 'null'

        # Upload Date
        if soup.find('div', {'id': 'watch-uploader-info'}) is not None:
            upload = soup.find('div', {'id': 'watch-uploader-info'}).get_text()
            if re.search(r"[0-9]+.[a-zA-Z]+.[0-9]+", upload) is not None:
                date_text = convertDate(str(re.findall(r"[0-9]+.[a-zA-Z]+.[0-9]+", upload)))
            else:
                date_text = date.today().strftime('%d-%b-%Y')
                print(date_text.replace('-', ' '))
            details[2] = " ".join(date_text.split())
        else:
            print('Did not find upload')
        if details[2] == '':
            details[2] = 'null'

        # Description
        if soup.find('div', {'id': 'watch-description-text'}) is not None:
            desc = soup.find('div', {'id': 'watch-description-text'}).get_text()
            details[3] = " ".join(desc.split())
        else:
            print('Did not find desc')
        if details[3] == '':
            details[3] = 'null'

        # Video ID
        vid_id = youtube_url_list.replace('https://www.youtube.com/watch?v=', '')
        details[4] = vid_id

        # URL
        details[5] = youtube_url_list

        # Country
        with open('E:/Scrape/Youtube/FullTest/' + filename + '.csv', 'rt', encoding='utf-8-sig',
                  newline='') as link:
            reader = csv.reader(link)
            for row in reader:
                if youtube_url_list == row[0]:
                    details[6] = row[1]
                    break
        print(details)
        time.sleep(3)
        break

    # When Youtube videos are unavailable due to being removed, return None as a condition to avoid writing it to file.
    if details[0] == 'null':
        return None
    else:
        return details


def main():
    start = timeit.default_timer()

    # Create the YoutubeLink file.
    with open('E:/Scrape/Youtube/FullTest/YoutubeVideoLinks.csv', 'wt') as Linklist:
        Linklist.close()

    # Create an output file
    with open('E:/Scrape/Youtube/FullTest/Uni_YoutubeData.csv', 'wt', encoding='utf-8', newline='') as website:
        writer = csv.writer(website)
        writer.writerow(['Channel_name', 'VideoTitle', 'Upload Date', 'VideoDescription', 'VideoID', 'URL', 'Country'])

    # Collect University Channel links
    with open('E:/Dropbox/Scrapping/YoutubeUni/ChannelList/TestList.csv', 'rt', encoding='utf-8',
              newline='') as ch_link:
        reader = csv.reader(ch_link)
        counter = 0
        ch_list = []
        country_list = []

        for row in reader:
            if 'http' in row[1]:
                ch_list.append(row[1])  # Taking Youtube links from the file.
                country_list.append(row[6])  # Taking Country Code from the file.
                counter += 1
        url = deque(ch_list)
        ctry = deque(country_list)
    print(ch_list)
    print(country_list)
    print('Number of links from school_videos file: ' + str(len(url)))

    # Collect all Youtube links from the University channel page.
    linknum = 0
    while (len(url)) != 0:
        linknum += 1
        print('Link no: ' + str(linknum))
        collect_links(url.pop(), ctry.pop())

    # Read information from collected Youtube links file 'YoutubeVideoLinks.csv'.
    filename = 'YoutubeVideoLinks'
    with open('E:/Scrape/Youtube/FullTest/' + filename + '.csv', 'rt', encoding='utf-8-sig',
              newline='') as link:
        reader = csv.reader(link)
        counter = 0  # Count how many links from the file.
        youtube_url_list = []  # Stores all URL links from file.

        for row in reader:
            youtube_url_list.append(row[0])
            counter += 1

        print('Total links read from ' + filename + '.csv are: ' + str(len(youtube_url_list)))

    # Define array to store all data from method collect_data() prior to writing to output file.
    all_data = multi_pool(collect_data, youtube_url_list, filename, 21)
    blank_count = 0                 # Counter for broken video links.
    with open('E:/Scrape/Youtube/FullTest/Uni_YoutubeData.csv', 'at', encoding="utf-8", newline='') as website:
        writer = csv.writer(website)
        print("Writing details to CSV File now....")
        for a in all_data:
            if a is not None:       # Skips any entries that are blank possibly due to Youtube video unavailable.
                writer.writerow(a)  # Writes data to Uni_YoutubeData .csv file
            else:
                blank_count += 1
        print("Total number of links read from YoutubeVideoLinks.csv file: " + str(len(all_data)))
        print("Total number of rows written to Uni_YoutubeData.csv file: " + str(len(all_data) - blank_count))
        print("Number of broken links found: " + str(blank_count))

    stop = timeit.default_timer()
    time_sec = stop - start
    time_min = int(time_sec / 60)
    time_hour = int(time_min / 60)

    time_run = str(format(time_hour, "02.0f")) + ':' + str(
        format((time_min - time_hour * 60), "02.0f") + ':' + str(format(time_sec - (time_min * 60), "^-05.1f")))
    print("This code has completed running in: " + time_run)

if __name__ == '__main__':
    main()