
import os



def get_sec(time_str):
  #print("##get_sec::time_str[{}]".format(time_str))
   
  time_sliced = time_str.split(':')
  if len(time_sliced) > 2:
    h, m, s = time_str.split(':')
  else:
    h = 0
    m, s = time_str.split(':')
   
  #print("##h[{}] m[{}] s[{}]".format(h,m,s))
  #err
   
  return int(h) * 3600 + int(m) * 60 + int(float(s))
