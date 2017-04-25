# -*- coding: utf-8 -*-
#! /usr/bin/env python3

class DecoWithArgs(object):

    
    def __init__(self):
        
        print('inside class __init__')
        #$self.args1 = args1
        #self.args2 = args2
        #self.args3 = args3
        #self.func = func
    

    def __call__(self, func):



        print('inside __call__  %s' % str(func))

        def warap(*args):

            func(*args)
        return warap

        #self.func(*args)
        #print('exit __call__ ')


       # def wrapper_func():
       #     print('inside __call__ %s' % self.func.__name__)
            
       # return wrapper_func

       

        
        #self.func(*args, **kwagrs)
            



@DecoWithArgs()
def g(*args):
    print('in function')


g('sd','sdds','sadasd')

s = lambda x: 1
d = s(13)
print(d)