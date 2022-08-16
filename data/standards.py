
# Scrap Meta Data model

SCRAP_META = [['scrap_meta', 'guid'], ['scrap_meta', 'date_start'],
            ['scrap_meta', 'maincategory_url'], ['scrap_meta', 'spider_country'],
            ['scrap_meta', 'spider_date_end'], ['scrap_meta', 'spider_marketplace'],
            ['scrap_meta', 'spider_name'], ['scrap_meta', 'spider_version'],
            ['scrap_meta', 'title'], ['scrap_meta', 'spider_date_start']]

# Product Data model
product_datamodel = {
                "sku":str,
                "title":str,
                "category":str,
                "source_category_url":str,
                "product_pos_in_page":int,
                "product_page":int,
                "product_url":str,
                "confs":str,
                "hasVariants":bool,
                "reviews_rating":float,
                "reviews_count":int,
                "currency":str,
                "img_url":str,
                "img_urls":str,
                "seller":str,  
                "price":float,
                "EAN":str,
                "description":str,
                "isAvailableInShop":bool,
                "isConfigurable":bool,
                "isStoreBrand":bool,  
                "isAvailableOnline":bool,
                "isSpecialPrice":bool,
                "onlineShippingCost":str,
                "onlineShippingLeadtime":str,
                "clickCollectLeadtime":str,
                "clickAndCollectState":str,
                "clickAndCollectAvailableQuantity":str,
                "deliveryTimeText":str,
                "specialPrice":float,
                "brand":str,
                "reviews":str,
                "scrap_meta.guid":str,
                "scrap_meta.maincategory_url":str,
                "scrap_meta.spider_country":str,
                "scrap_meta.spider_date_start":str,
                "scrap_meta.spider_date_end":str,
                "scrap_meta.spider_marketplace":str,
                "scrap_meta.spider_name":str,
                "scrap_meta.spider_version":str,
                "scrap_meta.title":str
}


#"cct":float,