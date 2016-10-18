#!/usr/bin/env python
# encoding: utf8
import os
from flanker import mime
from flanker.addresslib import address

def read(fileName):
    result = ''
    filepath = os.path.join('./emls/', fileName)
    with open(filepath, 'r') as fd:
        for line in fd.readlines():
            result += line
    return result

def export_csv(csvfile, data):
	exportfile = os.path.join('./', csvfile)
	with open(exportfile, 'a') as fd:
		fd.write(data)

for i in range(1,22455):
	filename = '{}.eml'.format(i)

	# print "parser: ", filename

	mime_msg = read(filename)
	msg = mime.from_string(mime_msg)

	# print msg.headers
	# print msg.subject

	# # print msg.content_type.is_singlepart()
	# if msg.content_type.is_multipart():
	# 	for part in msg.parts:
	# 		print part, part.size
	# else:
	# 	print msg.body, msg.size

	# print dir(msg)

	from_str = msg.headers.get('From')
	from_list = address.parse_list(from_str)

	from_email = ''
	if from_list:
		for from_addr in from_list.container:
			from_email = from_addr.address

	to_str = msg.headers.get('To')
	to_list = address.parse_list(to_str)

	cc_str = msg.headers.get('Cc')
	cc_list = address.parse_list(cc_str)

	if not cc_list:
		cc_email = ''

		if not to_list:
			to_email = ''
			csv_line = ', '.join([from_email, to_email, cc_email, str(i)])
			csv_line += '\r\n'
			export_csv('dnc.csv', csv_line)

		else:
			for to_addr in to_list.container:
				to_email = to_addr.address

				csv_line = ', '.join([from_email, to_email, cc_email, str(i)])
				csv_line += '\r\n'
				export_csv('dnc.csv', csv_line)

	else:
		for cc_addr in cc_list.container:
			cc_email = cc_addr.address

			if not to_list:
				to_email = ''
				csv_line = ', '.join([from_email, to_email, cc_email, str(i)])
				csv_line += '\r\n'
				export_csv('dnc.csv', csv_line)

			else:
				for to_addr in to_list.container:
					to_email = to_addr.address

					csv_line = ', '.join([from_email, to_email, cc_email, str(i)])
					csv_line += '\r\n'
					export_csv('dnc.csv', csv_line)


	# for to_addr in to_list.container:
	# 	print from_email, to_addr.address
		# print "{} -- {} -- {}".format(
		# 	to_addr.address,
		# 	to_addr.display_name,
		# 	# to_addr.mailbox,
		# 	to_addr.hostname,
		# 	)

	# print cc_list.addresses
	# print cc_list.full_spec()
	# print cc_list.hostnames
	# print cc_list.to_ascii_list()
	# print cc_list.addr_types
	# print cc_list.to_unicode()

	# if not cc_list.container:
	# 	for to_addr in to_list.container:
	# 		csv_line = ', '.join([from_email, to_addr.address, '', str(i)])
	# 		csv_line += '\r\n'
	# 		# export_csv('dnc.csv', csv_line)

	# for cc_addr in cc_list.container:
	# 	for to_addr in to_list.container:
	# 		csv_line = ', '.join([from_email, to_addr.address, cc_addr.address, str(i)])
	# 		csv_line += '\r\n'
	# 		# export_csv('dnc.csv', csv_line)

		# print "{} -- {} -- {}".format(
		# 	cc_addr.address,
		# 	cc_addr.display_name,
		# 	# cc_addr.mailbox,
		# 	cc_addr.hostname,
		# 	)
		# print dir(cc_addr)

	# print '发件人:', msg.headers.get('From')
	# print '收件人:', msg.headers.get('To')
	# print '抄送:',	msg.headers.get('Cc')
