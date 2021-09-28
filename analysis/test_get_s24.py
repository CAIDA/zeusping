
import sys

addr = 1180675258

s24_mask = 0
for i in range(24):
    s24_mask |= 1 << i

s24_mask = s24_mask << 8

# oct4_mask = 0
# for i in range(8):
#     oct4_mask |= 1 << i

oct4_mask = (1 << 8) - 1

s24 = addr & s24_mask
oct4 = addr & oct4_mask

print s24
print oct4


