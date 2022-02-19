#!/usr/bin/env python3

import xml.etree.ElementTree as etree
import os
import mwparserfromhell
import concurrent.futures



pathWikiXML = './simplewiki-latest-pages-articles.xml'
pathOut = './wiki-cleaned.txt'
separator = '+ * + * + * + * + * + * + * + * + * + *'



if not os.path.isfile(pathWikiXML):
	print('Missing input file ',pathWikiXML)
	print('You can download it from https://dumps.wikimedia.org/simplewiki/')
	exit()



#def convert(id, title, text):
def convert(arr):
	id, title, txt = arr
	parsed_wikicode = mwparserfromhell.parse(txt)
	txt = parsed_wikicode.strip_code()
	return id, title, txt


def clearTag(tag):
	idx = tag.rfind('}')
	if idx != -1:
		return tag[idx+1:]
	return tag




def main():
	global futures
	executor = concurrent.futures.ProcessPoolExecutor()
	futures = []
	
	fOut = open(pathOut, 'w')
	def cleanFutures():
		global futures
		for f in futures:
			res = f.result()
			id, title, txt = res

			fOut.write(title)
			fOut.write('\n')
			fOut.write(str(id))
			fOut.write('\n')
			fOut.write(txt)
			fOut.write('\n')
			fOut.write(separator)
			fOut.write('\n')
		futures = []


	total_size = os.path.getsize(pathWikiXML)
	lastPrc = 0

	with open(pathWikiXML, 'r') as f:
		path = []
		context = etree.iterparse(f, events=('start', 'end'))
		for event, elem in context:
			tag = clearTag(elem.tag)
			
			if event == 'start':
				path.append(tag)
				if tag == 'page' and len(path) == 2:
					title = ''
					id = -1
					ns = -1
					text = ''
			else:
				if len(path) == 2:
					if tag == 'page':
						if ns == 0:
							# write to file
							arr = (id,title,text)
							future = executor.submit(convert, arr)
							futures.append(future)
							if len(futures) >= 300:
								cleanFutures()
								prc = round(f.tell()/total_size*100)
								if prc != lastPrc:
									print(prc,'%')
									lastPrc = prc
				if len(path) == 3:
					if tag == 'title':
						title = ' '.join(elem.text.split())
					elif tag == 'ns':
						ns = int(elem.text)
					elif tag == 'id':
						id = int(elem.text)
				if len(path) == 4:
					if tag == 'text' and path[-2] == 'revision':
						text = elem.text

				path.pop()


	executor.shutdown()

	#s.runAll()



if __name__ == '__main__':
	main()
