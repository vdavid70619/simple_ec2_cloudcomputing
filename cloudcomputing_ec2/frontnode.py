#!/usr/bin/python

#Scientific Cloud Computing Architecture  
#Xiyang Dai    
#N19871532
#2012.8.15
#Instructor: Prof. J.C.Franchitti

#Front Node Script 

from BaseHTTPServer import BaseHTTPRequestHandler
import cgi
import control

class PostHandler(BaseHTTPRequestHandler):
    
    def do_POST(self):
        # Parse the form data posted
        form = cgi.FieldStorage(
            fp=self.rfile, 
            headers=self.headers,
            environ={'REQUEST_METHOD':'POST',
                     'CONTENT_TYPE':self.headers['Content-Type'],
                     })

        # Read information about what was posted in the form
        for field in form.keys():
            field_item = form[field]
            if field_item.filename:
                # The field contains an uploaded file
                print 'Post a file?!'
            else:
                # Regular form value
                print field + ' = ' + form[field].value
        if form['message'].value=='getall':    
            self.send_response(200)
            self.end_headers()
            self.wfile.write(control.get_all_tasks())
        elif form['message'].value=='changetask':  
            self.send_response(200)
            self.end_headers()
            self.wfile.write(control.apply_task_change(form['info'].value))
        elif form['message'].value=='checkname':
            self.send_response(200)
            self.end_headers()
            self.wfile.write(control.check_name(form['info'].value))
        elif form['message'].value=='newtask':
            self.send_response(200)
            self.end_headers()
            self.wfile.write(control.run_newtask(form['info'].value))
        else:
            self.send_response(200)
            self.end_headers()
            self.wfile.write('Nothing')
        
        return

if __name__ == '__main__':
    from BaseHTTPServer import HTTPServer
    server = HTTPServer(('ec2_ip', 1234), PostHandler)
    print 'Starting server, use <Ctrl-C> to stop'
    server.serve_forever()