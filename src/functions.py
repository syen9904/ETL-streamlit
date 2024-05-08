import os
def getCodes(directory):
    codes = {}
    for root, _, files in os.walk(directory):
        for file in files:
            if file.find('-checkpoint.') > 0: continue
            if file[::-1].find('.scala'[::-1]) == 0 or file[::-1].find('.sql'[::-1]) == 0:
                codes[file] = {}
                codes[file]['path'] = root+ '/' + file
                codes[file]['language'] = 'scala' if file[::-1].find('.scala'[::-1]) == 0 else 'sql'
                codes[file]['category'] = 'raw2pmap' if file.find('derived') == 0 else 'pmap2omop'
    return codes
