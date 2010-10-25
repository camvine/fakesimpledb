#!/usr/bin/env python
#
# Test implementation of simpledb's API that maps on an sqlite database
# per domain. It follows the API specified here:
#
# http://docs.amazonwebservices.com/AmazonSimpleDB/2009-04-15/DeveloperGuide/
#
# Requires jinja2, cherrypy, and sqlite3
#
# Michael Dales (mwd@camvine.com)
#
# Copyright (c) 2010 Cambridge Visual Networks Ltd.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

import cherrypy
import jinja2
import os
import re
import sqlite3
import uuid

# this is to mimic AWS's domain cap
DOMAIN_CAP = 100


BIND_ADDR = '0.0.0.0'  # Address to bind() and listen() on
try:
    SERVER_PORT = int(os.environ['FAKESIMPLEDB_PORT'])
except KeyError:
    SERVER_PORT = 8080

try:
    DATA_DIR = os.environ['FAKESIMPLEDB_DATA_DIR']
except KeyError:
    DATA_DIR = os.path.join(os.getcwd(), 'fakesimpledbdata')

try:
    TEMPLATE_DIR = os.environ['FAKESIMPELDB_TEMPLATE_DIR']
except KeyError:
    TEMPLATE_DIR = os.path.join(os.getcwd(), 'templates')

##############################################################################
##############################################################################

class AWSErrorException(Exception):
    
    InvalidParameterValue = "InvalidParameterValue"
    MissingParameter = "MissingParameter"
    NumberDomainsExceeded = "NumberDomainsExceeded"
    
    def __init__(self, error_code, error_message):
        self.error_code = error_code
        self.error_message = error_message
    
    def __unicode__(self):
        return "AWS Error %s: %s" % (self.error_code, self.error_message)
    


##############################################################################
##############################################################################

def render_to_string(template_name, params={}):
    
    # stock param
    params['request_id'] = uuid.uuid4()
    
    filename = os.path.join(TEMPLATE_DIR, template_name)
    template = jinja2.Template(open(filename).read())
    result = template.render(**params)
    return result

##############################################################################
##############################################################################


def create_domain(DomainName):
    
    # check the domain meets the restrictions for domain names set in the 
    # amazon API 
    # http://docs.amazonwebservices.com/AmazonSimpleDB/latest/DeveloperGuide/    
    checkre = re.compile('^[a-zA-Z0-9_\-\.]{3,255}$')
    match = checkre.match(DomainName)
    if not match:
        raise AWSErrorException(AWSErrorException.InvalidParameterValue,
            "Value (%s) for parameter DomainName is invalid." % DomainName)
    
    # check we're not using too many domains
    domains = list_domains()
    if len(domains) >= DOMAIN_CAP:
        raise AWSErrorException(AWSErrorException.NumberDomainsExceeded, 
            "Number of domains limit exceeded.")
    
    db_name = os.path.join(DATA_DIR, DomainName)
    conn = sqlite3.connect(db_name)
    conn.commit()
    
    
def delete_domain(DomainName):
    db_name = os.path.join(DATA_DIR, DomainName)
    try:
        os.unlink(db_name)
    except OSError:
        pass
    
def list_domains():
    return os.listdir(DATA_DIR)
        
def delete_attributes(DomainName, ItemName):    
    db_name = os.path.join(DATA_DIR, DomainName)
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    
    c.execute('delete from datatable where sdbkey=?', (ItemName,))
    
    conn.commit()
    
def put_attributes(DomainName, ItemName, **kwargs):
    
    # step 1 is to turn the attributes into a sane dictionary we can work with
    # the args are listed Attribute.0.name and Attribute.0.Value etc.
    
    attributes = {}
    index = 0
    while 1:
        if not kwargs.has_key('Attribute.%d.Name' % index):
            break
        attributes[kwargs['Attribute.%d.Name' % index]] = kwargs['Attribute.%d.Value' % index]
        index += 1
    
    if len(attributes) == 0:
        return
    
    # now to shove these into sqlite. we lazily create the table if there's 
    # no data in there yet 
    db_name = os.path.join(DATA_DIR, DomainName)
    conn = sqlite3.connect(db_name)
    
    sorted_keys = attributes.keys()
    sorted_keys.sort()
    
    c = conn.cursor()
    table_info = 'sdbkey text, '
    for key in sorted_keys:
        table_info += '%s text, ' % key
    query = 'create table if not exists datatable (%s)' % table_info[:-2]
    c.execute(query)
    
    value_tempate = '?, '
    value_list = [ItemName,]
    for key in sorted_keys:
        value_tempate += "?, "
        value_list.append(attributes[key])
    c.execute('insert into datatable values (%s)' % value_tempate[:-2], value_list)
    
    conn.commit()
    c.close()
    
def batch_put_attributes(DomainName, **kwargs):
    
    # here we just slice it up and call put_attributes repeatedly
    index = 0
    while 1:
        if not kwargs.has_key('Item.%d.ItemName' % index):
            break
        ItemName = kwargs['Item.%d.ItemName' % index]
        attributes = {}
        for key in kwargs:
            if key.startswith('Item.%d' % index):
                attributes[key.replace('Item.%d.' % index, '')] = kwargs[key]
        put_attributes(DomainName, **attributes)
        index += 1
    

def get_attributes(DomainName, ItemName):
    db_name = os.path.join(DATA_DIR, DomainName)
    conn = sqlite3.connect(db_name)
    
    attrs = {}
    
    c = conn.cursor()
    
    c.execute('select * from datatable where sdbkey=?', (ItemName,))
    
    # should be a unique response
    try:
        row = c.fetchall()[0]
    except IndexError:
        return attrs
        
    # we also need the column names
    c.execute('PRAGMA table_info(datatable)')
    columns = [x[1] for x in c.fetchall()]
    
    for i in xrange(len(columns)):
        if columns[i] == 'sdbkey':
            continue
        attrs[columns[i]] = row[i]
    
    conn.commit()
    c.close()
    
    return attrs
    
    
def select_items(SelectExpression):
    
    # the domain name is hidden in the expression
    r = re.compile("""(select|SELECT).*(from|FROM)\s['"`]{0,1}([a-zA-Z_0-9]+)['"`]{0,1}\s.*""")
    parts = r.match(SelectExpression).groups()
    DomainName = parts[2]
    
    db_name = os.path.join(DATA_DIR, DomainName)
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    
    query = SelectExpression.replace(DomainName, 'datatable')
    c.execute(query)
    query_results = c.fetchall()
    
    # we also need the column names
    c.execute('PRAGMA table_info(datatable)')
    columns = [x[1] for x in c.fetchall()]
    
    results = []
    for row in query_results:
        attrs = {}
        for i in xrange(len(columns)):
            attrs[columns[i]] = row[i]
        results.append(attrs)
    
    conn.commit()
    c.close()

    return results
    
    
##############################################################################
##############################################################################

class SimpleDBServer(object):
    
    def index(self, Action, **kwargs):
        
        try:        
            if Action == 'CreateDomain':
                create_domain(kwargs['DomainName'])
                return render_to_string('CreateDomain.xml')   
            elif Action == 'DeleteDomain':
                delete_domain(kwargs['DomainName'])
                return render_to_string('DeleteDomain.xml')             
            elif Action == 'ListDomains':
                domains = list_domains()
                return render_to_string('ListDomains.xml', {'domain_list': domains})
            elif Action == 'DeleteAttributes':
                delete_attributes(kwargs['DomainName'], kwargs['ItemName'])
                return render_to_string('DeleteAttributes.xml')
            elif Action == 'PutAttributes':
                put_attributes(**kwargs)
                return render_to_string('PutAttributes.xml')
            elif Action == 'GetAttributes':
                attibutes = get_attributes(kwargs['DomainName'], kwargs['ItemName'])
                return render_to_string('GetAttributes.xml', {'attrs': attibutes})
            elif Action == 'BatchPutAttributes':
                batch_put_attributes(**kwargs)
                return render_to_string('BatchPutAttributes.xml')            
            elif Action == 'Select':
                items = select_items(kwargs['SelectExpression'])
                return render_to_string('Select.xml', {'items': items})
            else:
                print Action
                print kwargs
                return "like, whatever."
        except AWSErrorException, e:
            return render_to_string('ErrorResponse.xml', {'exception': e})
    index.exposed = True

##############################################################################
##############################################################################

if __name__ == "__main__":
    try:
        os.makedirs(DATA_DIR)
    except OSError:
        pass

    # Global configuration
    cherrypy.config.update({'server.socket_host': BIND_ADDR,
                            'server.socket_port': SERVER_PORT})
    # Run server
    cherrypy.quickstart(SimpleDBServer())
