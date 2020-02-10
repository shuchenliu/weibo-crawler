import time
import xlrd
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import os
import excelSave as save

# 用来控制页面滚动
def Transfer_Clicks(browser):
    try:
        browser.execute_script("window.scrollBy(0,document.body.scrollHeight)", "")
    except:
        pass
    return "Transfer successfully \n"

#判断页面是否加载出来
def isPresent():
    temp =1
    try: 
        driver.find_elements_by_css_selector('div.line-around.layout-box.mod-pagination > a:nth-child(2) > div > select > option')
    except:
        temp =0
    return temp

#把超话页面滚动到底
def SuperwordRollToTheEnd():
    before = 0 
    after = 0
    n = 0
    timeToSleep = 50
    while True:
        before = after
        Transfer_Clicks(driver)
        time.sleep(3)
        elems = driver.find_elements_by_css_selector('div.m-box')
        print("当前包含超话最大数量:%d,n当前的值为:%d,当n为15无法解析出新的超话" % (len(elems),n))
        after = len(elems)
        if after > before:
            n = 0
        if after == before:        
            n = n + 1
        if n == 15:
            print("当前包含最大超话数为：%d" % after)
            break
        if after > timeToSleep:
            print("抓取到%d多条超话，休眠30秒" % timeToSleep)
            timeToSleep = timeToSleep + 50
            time.sleep(30)
#插入数据
def insert_data(elems,path,name):
    for elem in elems:
        workbook = xlrd.open_workbook(path)  # 打开工作簿
        sheets = workbook.sheet_names()  # 获取工作簿中的所有表格
        worksheet = workbook.sheet_by_name(sheets[0])  # 获取工作簿中所有表格中的的第一个表格
        rows_old = worksheet.nrows  # 获取表格中已存在的数据的行数       
        rid = rows_old + 1
        #用户名
        weibo_username = elem.find_elements_by_css_selector('h3.m-text-cut')[0].text
        weibo_userlevel = "普通用户"
        #微博等级
        try: 
            weibo_userlevel_color_class = elem.find_elements_by_css_selector("i.m-icon")[0].get_attribute("class").replace("m-icon ","")
            if weibo_userlevel_color_class == "m-icon-yellowv":
                weibo_userlevel = "黄v"
            if weibo_userlevel_color_class == "m-icon-bluev":
                weibo_userlevel = "蓝v"
            if weibo_userlevel_color_class == "m-icon-goldv-static":
                weibo_userlevel = "金v"
            if weibo_userlevel_color_class == "m-icon-club":
                weibo_userlevel = "微博达人"     
        except:
            weibo_userlevel = "普通用户"
        #微博内容
        # maybe should get a list of links first, then click into each weibo one by one
        # elemFull = elem.find_elements_by_xpath("//*[contains(text(), '全文')]").click()
        weibo_content = elem.find_elements_by_css_selector('div.weibo-text')[0].text
        shares = elem.find_elements_by_css_selector('i.m-font.m-font-forward + h4')[0].text
        comments = elem.find_elements_by_css_selector('i.m-font.m-font-comment + h4')[0].text
        likes = elem.find_elements_by_css_selector('i.m-icon.m-icon-like + h4')[0].text
        #发布时间
        weibo_time = elem.find_elements_by_css_selector('span.time')[0].text
        print("用户名："+ weibo_username + "|"
              "微博等级："+ weibo_userlevel + "|"
              "微博内容："+ weibo_content + "|"
              "转发："+ shares + "|"
              "评论数："+ comments + "|"
              "点赞数："+ likes + "|"
              "发布时间："+ weibo_time + "|"
              "话题名称" + name + "|" )
        value1 = [[rid, weibo_username, weibo_userlevel,weibo_content, shares,comments,likes,weibo_time,keyword,name],]
        print("当前插入第%d条数据" % rid)
        save.write_excel_xls_append_norepeat(book_name_xls, value1)
#获取当前页面的数据
def get_current_weibo_data(book_name_xls,name,maxWeibo):
    #开始爬取数据
        before = 0 
        after = 0
        n = 0 
        timeToSleep = 300
        while True:
            before = after
            Transfer_Clicks(driver)
            time.sleep(3)
            elems = driver.find_elements_by_css_selector('div.card.m-panel.card9')
            print("当前包含微博最大数量：%d,n当前的值为：%d, n值到15说明已无法解析出新的微博" % (len(elems),n))
            after = len(elems)
            if after > before:
                n = 0
            if after == before:        
                n = n + 1
            if n == 15:
                print("当前关键词最大微博数为：%d" % after)
                insert_data(elems,book_name_xls,name)
                break
            if len(elems)>maxWeibo:
                print("当前微博数以达到%d条"%maxWeibo)
                insert_data(elems,book_name_xls,name)
                break
            if after > timeToSleep:
                print("抓取到%d多条，插入当前新抓取数据并休眠30秒" % timeToSleep)
                timeToSleep = timeToSleep + 300
                insert_data(elems,book_name_xls,name) 
                time.sleep(30) 

#爬虫运行 
def spider(username,password,driver,book_name_xls,sheet_name_xls,keyword,maxWeibo):
    
    #创建文件
    if os.path.exists(book_name_xls):
        print("文件已存在")
    else:
        print("文件不存在，重新创建")
        value_title = [["rid", "用户名称", "微博等级", "微博内容", "微博转发量","微博评论量","微博点赞","发布时间","搜索关键词","话题名称"],]
        save.write_excel_xls(book_name_xls, sheet_name_xls, value_title)
    
    #加载驱动，使用浏览器打开指定网址  
    driver.set_window_size(452, 790)
    driver.get("https://passport.weibo.cn/signin/login?entry=mweibo&res=wel&wm=3349&r=https%3A%2F%2Fm.weibo.cn%2F")  
    time.sleep(3)
    #登陆
    elem = driver.find_element_by_xpath("//*[@id='loginName']");
    elem.send_keys(username)
    elem = driver.find_element_by_xpath("//*[@id='loginPassword']");
    elem.send_keys(password)
    elem = driver.find_element_by_xpath("//*[@id='loginAction']");
    elem.send_keys(Keys.ENTER) 
    time.sleep(5)
    #判断页面是否加载出
    while 1:  # 循环条件为1必定成立
        result = isPresent()
        print ('判断页面1成功 0失败  结果是=%d' % result )
        if result == 1:
            elems = driver.find_elements_by_css_selector('div.line-around.layout-box.mod-pagination > a:nth-child(2) > div > select > option')
            #return elems #如果封装函数，返回页面
            break
        else:
            print ('页面还没加载出来呢')
            time.sleep(20)
    time.sleep(10)
    driver.get("https://m.weibo.cn/p/index?containerid=1008084882401a015244a2ab18ee43f7772d6f&extparam=%E8%82%BA%E7%82%8E%E6%82%A3%E8%80%85%E6%B1%82%E5%8A%A9&luicode=10000011&lfid=100103type%3D1%26q%3D%E8%82%BA%E7%82%8E%E6%82%A3%E8%80%85%E6%B1%82%E5%8A%A9") 

    name = keyword

    get_current_weibo_data(book_name_xls,name, maxWeibo) #爬取综合
    time.sleep(3)
   
    
if __name__ == '__main__':
    username = os.environ["USER"] #你的微博登录名
    password = os.environ["PW"] #你的密码
    driver = webdriver.Chrome('./chromedriver')#你的chromedriver的地址
    book_name_xls = "./weibo.xls" #填写你想存放excel的路径，没有文件会自动创建
    sheet_name_xls = '微博数据' #sheet表名
    maxWeibo = 3000 #设置最多多少条微博，如果未达到最大微博数量可以爬取当前已解析的微博数量
    keywords = ["肺炎患者求助",] #输入你想要的关键字，可以是多个关键词的列表的形式
    for keyword in keywords:
        spider(username,password,driver,book_name_xls,sheet_name_xls,keyword,maxWeibo)
