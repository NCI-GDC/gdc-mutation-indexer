import os
import datetime

from tests_config import TestConfig


conf = TestConfig()
postfix = '_summary.log'

print "[{}] Index accuracy report".format(datetime.datetime.now().date())
for filename in sorted(os.listdir(conf.log_dir)):
    if filename.find(postfix) != -1:
        line_count = 0
        acc = []
        with open(os.path.join(conf.log_dir, filename), 'r') as f:
            for line in f.readlines():
                line_count += 1
                acc.append(float(line.split(',')[1]))

        print 'avg: {}, min: {}, max {} [{}]'\
              .format(round(sum(acc)/len(acc), 2), round(min(acc), 2),
                      round(max(acc), 2), filename.replace(postfix, '').upper())

