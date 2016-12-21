#!/usr/bin/python
# -*- coding: utf-8 -*-

default_application = 'app'

routes_app = ((r'/admin\b.*', r'admin'),
              (r'/app(.*)', r'app'))


# http://www.dev-explorer.com/articles/full-page-iframe
error_message_ticket="""
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd"> 
<html xmlns="http://www.w3.org/1999/xhtml" lang="EN"> 
<head> 
<meta http-equiv="Content-Type" content="text/html; charset=utf-8" /> 
<title>ERROR</title>

</head>
<body style="margin: 0px; padding: 0px; height: 100vh; border: none; overflow: auto;"> 
Error: <a href="/admin/default/ticket/%(ticket)s" target="_blank">%(ticket)s</a>
<iframe  src="/admin/default/ticket/%(ticket)s" style="display: block; width: 100%; border: none; overflow-y: auto; overflow-x: hidden;" frameborder="0" marginheight="0" marginwidth="0" width="100%" height="100%" scrolling="auto"></iframe> 
</body> 
</html>
""".replace("0%", "0%%")
