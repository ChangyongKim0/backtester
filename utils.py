def removeEmpty(text):
    return text.replace(' ','').replace(u'\xa0','').replace('\n','').replace('\t','').strip()
