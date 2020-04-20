# -*- coding: utf-8 -*-
import decimal
import json
import scrapy
import re
from urllib.parse import urljoin
from scrapy.selector import Selector
from shopping.models.schema import Category, Product, Price
from shopping.items import ProductItem, PriceItem
from shopping.util import FakeData
from shopping.util import Redis
from scrapy_redis.spiders import RedisCrawlSpider
from shopping.models.schema import db_connect
import hashlib


class XXXXSpider(RedisCrawlSpider):
    name = 'XXXX'
    allowed_domains = ['XXXXXX.cn']
    redis_key = 'shopping:start_urls'
    myRedis = Redis().myRedis
    myRedis.lpush("shopping:start_urls", 'http://www.XXXXX.cn/XXXX/')
    fakeData = FakeData()
    city = '111'
    shop = 'XXXXX'
    baseURL = 'http://list.XXXXXXX.cn/searchPage/'

    session = db_connect()
    catObj = session.query(Category).all()
    prodObj = session.query(Product).all()

    allCategoryIds = {
        c.hashed: c.published for c in catObj
    }

    allProductIds = {
        c.hashed: c.price for c in prodObj
    }

    """
    remove duplication when crawl
    """
    prodUrls = set()

    def parse(self, response):
        # ----- start to generate keywords -----
        pageType = 0
        pageId = 0

        items = response.xpath("//meta[@name='tp_page']/@content").extract_first()
        arr = self.fakeData.checkTpPage(items)
        # check arr return
        if len(arr) > 0:
            homePageRe = '"{0}":"(.*?)"'.format(arr[0])
            # find pageType
            res = re.search(homePageRe, response.text)
            if res is not None:
                pageType = res.group(1)
                pageId = arr[1]
            else:
                print('--- PageType not found ---')
        else:
            print('--- arr has something wrong---')

        # ----- finished generate keywords -----
        # gen fake tp info
        Obj = self.fakeData.genTP(pageType, pageId)
        tp = Obj['tp']
        ti = "{0}_{1}".format(Obj['unid'], self.fakeData.generateMixed(4))

        # -- start to crawl all menus --
        starter = response.xpath("//ul[contains(@class, 'global-nav-list')]/li[@class='li']").extract()
        for item in starter:
            rootName, rootUrl, rootIndex = self._parse(item, '//a/text()', '//a/@href', '/c(.*?)-1/')
            # skipp first menu
            if rootName == "XXXXX":
                continue
            h = self.md5URL(rootUrl)
            menuChecker = self._menuChecker(h)
            # allow to write to db

            if menuChecker is False:
                yield {
                    'hashed': h,
                    'name': rootName,
                    'id': rootIndex,
                    'url': rootUrl,
                    'published': True,
                    'parent': 0,
                    'city': self.city,
                    'shop': self.shop
                }

            dls = Selector(text=item).xpath("//dl").extract()
            for dl in dls:
                dtName, dtUrl, dtIndex = self._parse(dl, '//dt/a/text()', '//dt/a/@href', '/c(.*?)-1/')
                dds = Selector(text=dl).xpath("//dd").extract()

                h = self.md5URL(dtUrl)
                dtsChecker = self._menuChecker(h)

                # allow to write to db
                if dtsChecker is False:
                    yield {
                        'hashed': h,
                        'name': dtName,
                        'id': dtIndex,
                        'url': dtUrl,
                        'published': True,
                        'parent': rootIndex,
                        'city': self.city,
                        'shop': self.shop
                    }

                for dd in dds:
                    ddName, ddUrl, ddIndex = self._parse(dd, '//dd/a/text()', '//dd/a/@href', '/c(.*?)-1/')

                    # allow to write to db
                    h = self.md5URL(ddUrl)
                    ddsChecker = self._menuChecker(h)
                    if ddsChecker is False:
                        yield {
                            'hashed': h,
                            'name': ddName,
                            'id': ddIndex,
                            'url': ddUrl,
                            'published': True,
                            'parent': dtIndex,
                            'city': self.city,
                            'shop': self.shop
                        }

                    # request pages
                    tps = self.fakeData.getRateByPos()
                    url = "{0}?tp={1}&tps={2}&ti={3}".format(ddUrl, tp, tps, ti)
                    yield scrapy.Request(url, callback=self.catParse, meta={'hashed': h})

        # -- finish crawl all menus --

        # 3. old not in new, publish =false
        oldIds = self.allCategoryIds.keys()
        if len(oldIds) > 0:
            print('situation 3')
            for h in oldIds:
                tmp = self.session.query(Category).filter(Category.hashed == h).first()
                tmp.published = False
                self.update()

    @staticmethod
    def md5URL(url):
        return hashlib.md5(url.encode("utf8")).hexdigest()

    @staticmethod
    # findout all the menus
    def _parse(s, pa, pb, pc):
        name = Selector(text=s).xpath(pa).extract_first()
        url = urljoin('http:', Selector(text=s).xpath(pb).extract_first())
        index = int(re.search(pc, url).group(1))
        return name, url, index

    # check duplicate
    # 1: cur not in old: add
    # 2: cur in old: remove
    # 3. old not in new, publish =false
    def _menuChecker(self, index):
        if self.allCategoryIds:
            return self.allCategoryIds.pop(index, 0)
        else:
            return False

    def update(self):
        try:
            self.session.commit()
        except:
            self.session.rollback()
            raise
        finally:
            self.session.close()

    # for category page parse
    def catParse(self, response):
        # find catIndex from response
        baseIndex = response.meta['hashed']
        # start process
        starter = response.xpath("//div[contains(@class, 'jsModSearfhPro')]").extract()

        for item in starter:
            # init data from items.py
            product = ProductItem()
            product['city'] = self.city
            product['shop'] = self.shop
            # there is a limit, only response function can yield
            product['hashed'], product['price'], product['thumbnail'], \
                product['name'], product['type'], product['url'], product['id'] = self._parse_product(item)
            product['category'] = baseIndex
            product['published'] = True

            if self.duFilter(product['hashed']):
                if product['hashed'] not in self.allProductIds.keys():
                    # if hashed not in self.products:
                    # 1. new product 2. price or url changed
                    p = self.session.query(Product).filter(Product.id == product['id']).first()
                    if p is not None and p.price != product['price']:
                        # create history price
                        price = PriceItem()
                        price['product'] = p.id
                        price['price'] = p.price
                        # new price
                        yield price

                        p.latestRatio = round((decimal.Decimal(product['price'])-p.price)/p.price, 2)
                        p.price = product['price']
                        p.hashed = product['hashed']
                        self.update()

                    else:
                        # new product
                        yield product

        action = response.xpath("//a[@id='searchProductNext']/@url").extract_first()
        if action is not None:
            clickMore = "{0}{1}".format(self.baseURL, action)
            yield scrapy.Request(clickMore, callback=self.more, meta={'baseIndex': baseIndex}, dont_filter=True)

    def more(self, response):
        baseIndex = response.meta['baseIndex']
        type_json = json.loads(str(response.body, encoding='utf-8'))
        if type_json:
            action = Selector(text=type_json['value']).xpath("//a[@id='searchProductNext']/@url").extract_first()
            if action:
                clickMore = "{0}{1}".format(self.baseURL, action)
                yield scrapy.Request(clickMore, callback=self.more, meta={'baseIndex': baseIndex}, dont_filter=True)
            else:
                print('no more next page')

            starter = Selector(text=type_json['value']).xpath("//div[contains(@class, 'jsModSearfhPro')]").extract()
            for item in starter:
                # init data from items.py
                product = ProductItem()
                product['city'] = self.city
                product['shop'] = self.shop
                # there is a limit, only response function can yield
                # if hashed not in self.products:
                # 1. new product 2. price changed
                product['hashed'], product['price'], product['thumbnail'], \
                product['name'], product['type'], product['url'], product['id'] = self._parse_product(item)
                product['category'] = baseIndex
                product['published'] = True

                if self.duFilter(product['hashed']):
                    if product['hashed'] not in self.allProductIds.keys():
                    # if hashed not in self.products
                    # 1. new product 2. price or url changed
                        p = self.session.query(Product).filter(Product.id == product['id']).first()
                        if p is not None and p.price != product['price']:
                            # create history price
                            price = PriceItem()
                            price['product'] = p.id
                            price['price'] = p.price
                            # new price
                            yield price

                            p.latestRatio = round((decimal.Decimal(product['price']) - p.price) / p.price, 2)
                            p.price = product['price']
                            p.hashed = product['hashed']
                            self.update()

                        else:
                            # new product
                            yield product


    #remove duplication
    def duFilter(self, key):
        a = len(self.prodUrls)
        self.prodUrls.add(key)
        b = len(self.prodUrls)
        if a == b:
            return False
        else:
            return True

    @staticmethod
    def _parse_product(item):
        # -- start parse products --
        hashed = ''
        productType = 'common'
        url = urljoin('http:',
                      Selector(text=item).xpath("//p[contains(@class, 'proName')]/a/@href").extract_first())
        id = url.split('/')[3]
        name = Selector(text=item).xpath("//p[contains(@class, 'proName')]/a/text()").extract_first()
        tag = Selector(text=item).xpath("//p[contains(@class, 'proName')]/u/@class").extract_first()
        if tag == 'jsd-tag':
            productType = 'jsd'
        elif tag == 'qqg-tag':
            productType = 'qqg'
        if Selector(text=item).xpath("//img[@class='lazyload']").extract_first():
            thumbnail = urljoin('http:', Selector(text=item).xpath("//img/@original").extract_first())
        else:
            thumbnail = urljoin('http:', Selector(text=item).xpath("//img/@src").extract_first())

        price = Selector(text=item).xpath("//p[contains(@class, 'proPrice')]/em/text()").extract_first()
        if url and price:
            hashed = hashlib.md5("{0}{1}".format(url, price).encode("utf8")).hexdigest()
        else:
            print("---- can not get url or price for product---")
        # -- finish parse products --

        return hashed, price, thumbnail, name, productType, url, id
