# -*- coding: utf-8 -*-
#! /usr/bin/env python3

class DecoWithArgs(object):

    
    def __init__(self,func):
        self.func = func
        print('inside class __init__')
    

    def __call__(self,*args, **kwargs):

        print('inside __call__ %s' % self.func.__name__,args ,kwargs)

        kwargs['s'] = 3334
        self.func(**kwargs)
            



@DecoWithArgs
def g(*args,**kwargs):
    print('in function')


g('d','ss',s=3,d=45)