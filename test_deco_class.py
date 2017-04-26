# -*- coding: utf-8 -*-
#! /usr/bin/env python3
import itertools

class DecoWithArgs(object):

    
    def __init__(self,args1,args2,args3):
        
        print('inside class __init__')
        #additional agrs in decorator
        self.args1 = str(args1)
        self.args2 = str(args2)
        self.args3 = str(args3)
        self.q_list = []

        self.q_list.append([x for x in itertools.chain(self.args1,self.args2,self.args3)])

        self.q_dict = {}
    

    def __call__(self, func):
        print('inside __call__  %s' % str(func))

        def warap(*args, **kwagrs):
            #args.append(12)
            print(args, kwagrs)
            ##  change args, kwargs on function   
            self.q_dict.update(kwagrs)

            d = {'ddx':'12'}
            
            self.q_list.append(['11','222','21'])
            self.q_dict.update(d)
            self.q_list.append(args)
            
            print(self.q_dict)
            func1 = func(*self.q_list, **self.q_dict)
            return func1
        return warap
           



@DecoWithArgs('aa','asda','fddss')
def g(*args, **kwagrs):
    print('in function, args %s' % str(args))
    return args,kwagrs


#return_deco_class = sorted(g('sddddd','sdds','sadasd'), key=lambda x:len(x))
return_deco_class = g('sddddd','sdds','sadasd',dd=3)
print(return_deco_class)

s = lambda x: 1
d = s(13)
print(d)