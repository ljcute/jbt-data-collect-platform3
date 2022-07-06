from selenium import webdriver


def get_driver():
    # 创建chrome参数对象
    option = webdriver.ChromeOptions()
    option.add_argument("--headless")
    option.add_argument("-incognito")
    option.binary_location = r'C:\Users\jbt\AppData\Local\Chromium\Application\Chromium.exe'
    driver = webdriver.Chrome(executable_path='./chromedriver.exe', chrome_options=option)
    return driver
