#coding:utf-8

import sys,os
reload(sys)
sys.setdefaultencoding('utf-8')
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

# from utils import common
import threading
import time
from datetime import datetime,timedelta
import cx_Oracle
from collections import OrderedDict
import math as Math

pi = 3.14159265358979324
a = 6378245.0
ee = 0.00669342162296594323
class EvilTransform(object):
  """docstring for EvilTransform"""
    # // Krasovsky 1940
    # //
    # // a = 6378245.0, 1/f = 298.3
    # // b = a * (1 - f)
    # // ee = (a^2 - b^2) / a^2


  def __init__(self, arg):
    super(EvilTransform, self).__init__()
    self.arg = arg

  # //
  # // World Geodetic System ==> Mars Geodetic System
  @staticmethod
  def transform(wgLat, wgLon):
    ret = [None,None]

    if EvilTransform.outOfChina(wgLat, wgLon):
      ret[0] = wgLat
      ret[1] = wgLon
      return ret

    dLat = EvilTransform.transformLat(wgLon - 105.0, wgLat - 35.0)
    dLon = EvilTransform.transformLon(wgLon - 105.0, wgLat - 35.0)
    radLat = wgLat / 180.0 * pi
    magic = Math.sin(radLat)
    magic = 1 - ee * magic * magic
    sqrtMagic = Math.sqrt(magic)
    dLat = (dLat * 180.0) / ((a * (1 - ee)) / (magic * sqrtMagic) * pi)
    dLon = (dLon * 180.0) / (a / sqrtMagic * Math.cos(radLat) * pi)

    ret[0] = wgLon + dLon
    ret[1] = wgLat + dLat

    return ret

  @staticmethod
  def outOfChina(lat, lon):
    """ 判断经纬度坐标是否在中国  """
    if (lon < 72.004) or (lon > 137.8347):
        return True
    if (lat < 0.8293) or (lat > 55.8271):
        return True
    return False

  @staticmethod
  def transformLat(x, y):
    """ 转换纬度 """
    ret = -100.0 + 2.0 * x + 3.0 * y + 0.2 * y * y + 0.1 * x * y + 0.2 * Math.sqrt(abs(x))
    ret += (20.0 * Math.sin(6.0 * x * pi) + 20.0 * Math.sin(2.0 * x * pi)) * 2.0 / 3.0
    ret += (20.0 * Math.sin(y * pi) + 40.0 * Math.sin(y / 3.0 * pi)) * 2.0 / 3.0
    ret += (160.0 * Math.sin(y / 12.0 * pi) + 320 * Math.sin(y * pi / 30.0)) * 2.0 / 3.0
    return ret

  @staticmethod
  def transformLon(x, y):
    """ 转换经度 """
    ret = 300.0 + x + 2.0 * y + 0.1 * x * x + 0.1 * x * y + 0.1 * Math.sqrt(abs(x))
    ret += (20.0 * Math.sin(6.0 * x * pi) + 20.0 * Math.sin(2.0 * x * pi)) * 2.0 / 3.0
    ret += (20.0 * Math.sin(x * pi) + 40.0 * Math.sin(x / 3.0 * pi)) * 2.0 / 3.0
    ret += (150.0 * Math.sin(x / 12.0 * pi) + 300.0 * Math.sin(x / 30.0 * pi)) * 2.0 / 3.0
    return ret


# class EhlHelper(object):
#     """docstring for EhlHelper"""
#     def __init__(self, name):
#         super(EhlHelper, self).__init__()
#         self.name = name

#         self.logger = common.getLogger('')

#         self.conn = {}
#         self.conn['tfm'] = common.getTfmOracle()
#         self.conn['itgs'] = common.getItgsOracle()

#         self.cfg = common.getConf()
#         self.table = common.getTables()

#         # self.logger.info("thread '%s' initialization finished." % (self.name))

#     def writeLinkDirEquip(self, fname, data):
#         from openpyxl import Workbook
#         import string
#         from openpyxl.cell import get_column_letter
#         from openpyxl.styles import *

#         wb = Workbook()

#         ws = wb.active
#         ws.title = u'试点路段判态依据'

#         indx = 1
#         cols = [u'编号',u'路段名称',u'设备类型',u'判态参数',u'交通指数权重',u'门限值']
#         ncols = len(cols)

#         cols_info = {
#             u'编号': 7.0,
#             u'路段名称': 38.0,
#             u'设备类型': 15.0,
#             u'判态参数': 18.0,
#             u'交通指数权重': 15.0,
#             u'门限值': 17.0,
#         }


#         # 计算列的宽度
#         # for col in cols_info.keys():
#         #     cols_info[col] = len(col)
#         #     print col,cols_info[col]

#         # ldirid_no = 1
#         # for ldirid,info in data.iteritems():
#         #     # print len(info['lname'])
#         #     # print 'cols info: ',cols_info

#         #     if len('%d' % ldirid_no) > cols_info[cols[0]]:
#         #         cols_info[cols[0]] = len('%d' % ldirid_no)
#         #     ldirid_no += 1

#         #     if len(info['lname']) > cols_info[cols[1]]:
#         #         cols_info[cols[1]] = len(info['lname'])
#         #         print cols[1], cols_info[cols[1]]

#         #     # for i in xrange(0,info['equip_cnt']):
#         #     for item in info['equip_info']:
#         #         # print '%d: ' % i
#         #         # print len(item['dtype']), len(item['algo']), len(item['weight'])

#         #         if (item['dtype'] is not None):
#         #             # print len(item['dtype']),cols_info[cols[2]]
#         #             if len(item['dtype']) > cols_info[cols[2]]:
#         #                 cols_info[cols[2]] = len(item['dtype'])
#         #                 # print cols[2], cols_info[cols[2]]

#         #         if (item['algo'] is not None):
#         #             # print len(item['algo']), cols_info[cols[3]]
#         #             if len(item['algo']) > cols_info[cols[3]]:
#         #                 cols_info[cols[3]] = len(item['algo'])
#         #                 # print cols[3], cols_info[cols[3]]

#         #         if (item['weight'] is not None):
#         #             # print len(item['weight']), cols_info[cols[4]]
#         #             if len(item['weight']) > cols_info[cols[4]]:
#         #                 cols_info[cols[4]] = len(item['weight'])
#         #                 # print cols[4], cols_info[cols[4]]


#         s_head = Style(font=Font(name='Arial', bold=True, size=12),
#                         alignment=Alignment(horizontal='center',vertical='center'),
#                         fill=PatternFill(fill_type=None,
#                                         start_color='FFAA0000',
#                                         end_color='FFAA0000'),
#                         border=Border(left=Side(border_style='medium',
#                                                 color='FF000000'),
#                                       right=Side(border_style='medium',
#                                                  color='FF000000'),
#                                       top=Side(border_style='medium',
#                                                color='FF000000'),
#                                       bottom=Side(border_style='medium',
#                                                   color='FF000000')))
#         # cols_indx = [c for c in string.ascii_uppercase[:ncols]]
#         for i,cname in enumerate(cols):
#             ws['%s%d' % (get_column_letter(i+1),indx)] = cname
#             ws['%s%d' % (get_column_letter(i+1),indx)].style = s_head
#         ws.row_dimensions[1].height = 35.0
#         indx += 1

#         ldirid_no = 1
#         for ldirid, info in data.iteritems():
#             cnt = info['equip_cnt']
#             # ws.merge_cells(start_row=indx,start_column=1,end_row=indx+cnt-1,end_column=1)

#             ws.merge_cells('A%d:A%d' % (indx,indx+cnt-1))
#             ws.merge_cells('B%d:B%d' % (indx,indx+cnt-1))

#             # 序号
#             ws['A%d' % (indx)] = ldirid #ldirid_no

#             # 路段名称
#             ws['B%d' % (indx)] = info['lname']

#             style = Style(alignment=Alignment(horizontal='left',vertical="center"),
#                         border=Border(left=Side(border_style='thin',
#                                                         color='FF000000'),
#                                               right=Side(border_style='thin',
#                                                          color='FF000000'),
#                                               # top=Side(border_style='thin',
#                                                        # color='FF000000'),
#                                               bottom=Side(border_style='dashed',
#                                                           color='FF000000')))
#             style1 = Style(alignment=Alignment(horizontal='left',vertical="center"),
#                             border=Border(left=Side(border_style='thin',
#                                                             color='FF000000'),
#                                           right=Side(border_style='thin',
#                                                      color='FF000000'),
#                                           # top=Side(border_style='thin',
#                                                    # color='FF000000'),
#                                           bottom=Side(border_style='thin',
#                                                       color='FF000000')))
#             s_rate = Style(alignment=Alignment(horizontal='right',vertical="center"),
#                             border=Border(left=Side(border_style='thin',
#                                                         color='FF000000'),
#                                           right=Side(border_style='thin',
#                                                      color='FF000000'),
#                                           bottom=Side(border_style='dashed',
#                                                       color='FF000000')))
#             s_rate1 = Style(alignment=Alignment(horizontal='right',vertical="center"),
#                             border=Border(left=Side(border_style='thin',
#                                                         color='FF000000'),
#                                           right=Side(border_style='thin',
#                                                      color='FF000000'),
#                                           bottom=Side(border_style='thin',
#                                                       color='FF000000')))

#             for i in xrange(0,cnt):
#                 ws.cell('A%d' % (indx+i)).style = Style(alignment=Alignment(vertical="center",horizontal='center'),
#                                                     border=Border(left=Side(border_style='thin',
#                                                                             color='FF000000'),
#                                                                   right=Side(border_style='thin',
#                                                                              color='FF000000'),
#                                                                   # top=Side(border_style='thin',
#                                                                   #          color='FF000000'),
#                                                                   bottom=Side(border_style='thin',
#                                                                               color='FF000000')))

#                 ws.cell('B%d' % (indx+i)).style = Style(alignment=Alignment(vertical="center",horizontal='left'),
#                                                     border=Border(left=Side(border_style='thin',
#                                                                             color='FF000000'),
#                                                                   right=Side(border_style='thin',
#                                                                              color='FF000000'),
#                                                                   # top=Side(border_style='thin',
#                                                                   #          color='FF000000'),
#                                                                   bottom=Side(border_style='thin',
#                                                                               color='FF000000')))

#                 ws['C%d' % (indx+i)] = info['equip_info'][i]['dtype']
#                 ws.cell('C%d' % (indx+i)).style = style if i<(cnt-1) else style1

#                 ws['D%d' % (indx+i)] = info['equip_info'][i]['algo']
#                 ws.cell('D%d' % (indx+i)).style = style if i<(cnt-1) else style1

#                 ws['E%d' % (indx+i)] = info['equip_info'][i]['weight']
#                 ws.cell('E%d' % (indx+i)).style = style if i<(cnt-1) else style1

#                 ws['F%d' % (indx+i)] = info['equip_info'][i]['thresh']
#                 ws.cell('F%d' % (indx+i)).style = s_rate if i<(cnt-1) else s_rate1

#                 ws.row_dimensions[indx+i].height = 24.0

#             ldirid_no += 1
#             indx += cnt

#         # start, stop = 2, indx-1
#         # for index,row in enumerate(ws.iter_rows()):
#         #     if start < index < stop:
#         #         for cell in row:
#         #             cell.style = None

#         for i,cname in enumerate(cols):
#             self.logger.info('%s, %s: %d' % (get_column_letter(i+1), cname, cols_info[cname]))
#             ws.column_dimensions[get_column_letter(i+1)].width = cols_info[cname]

#         # 应用样式
#         # ws.row_dimensions[1].style = s_head

#         wb.save(fname)
#         self.logger.info('write %s successfully.' % (fname))

#     def writeLinkDirEquip1(self, fname, data):
#         from openpyxl import Workbook
#         import string
#         from openpyxl.cell import get_column_letter
#         from openpyxl.styles import *

#         wb = Workbook()

#         ws = wb.active
#         ws.title = u'试点路段判态依据'

#         indx = 1
#         cols = [u'道路名称',u'路段编号',u'路段名称',u'设备类型总数',u'设备类型',u'设备编号',u'车道总数']
#         ncols = len(cols)

#         cols_info = {
#             u'道路名称': 7.0,
#             u'路段编号': 38.0,
#             u'路段名称': 15.0,
#             u'设备类型总数': 18.0,
#             u'设备类型': 15.0,
#             u'设备编号': 17.0,
#             u'车道总数': 15.0,
#         }


#         s_head = Style(font=Font(name='Arial', bold=True, size=12),
#                         alignment=Alignment(horizontal='center',vertical='center'),
#                         fill=PatternFill(fill_type=None,
#                                         start_color='FFAA0000',
#                                         end_color='FFAA0000'),
#                         border=Border(left=Side(border_style='medium',
#                                                 color='FF000000'),
#                                       right=Side(border_style='medium',
#                                                  color='FF000000'),
#                                       top=Side(border_style='medium',
#                                                color='FF000000'),
#                                       bottom=Side(border_style='medium',
#                                                   color='FF000000')))
#         # cols_indx = [c for c in string.ascii_uppercase[:ncols]]
#         for i,cname in enumerate(cols):
#             ws['%s%d' % (get_column_letter(i+1),indx)] = cname
#             ws['%s%d' % (get_column_letter(i+1),indx)].style = s_head
#         ws.row_dimensions[1].height = 35.0
#         indx += 1

#         ldirid_no = 1
#         for roadid, info in data.iteritems():
#             cnt = info['cnt']
#             # ws.merge_cells(start_row=indx,start_column=1,end_row=indx+cnt-1,end_column=1)

#             ws.merge_cells('A%d:A%d' % (indx,indx+cnt-1))
#             # ws.merge_cells('B%d:B%d' % (indx,indx+cnt-1))

#             # 序号
#             ws['A%d' % (indx)] = roadid #ldirid_no

#             # 路段名称
#             ws['B%d' % (indx)] = info['lname']

#             style = Style(alignment=Alignment(horizontal='left',vertical="center"),
#                         border=Border(left=Side(border_style='thin',
#                                                         color='FF000000'),
#                                               right=Side(border_style='thin',
#                                                          color='FF000000'),
#                                               # top=Side(border_style='thin',
#                                                        # color='FF000000'),
#                                               bottom=Side(border_style='dashed',
#                                                           color='FF000000')))
#             style1 = Style(alignment=Alignment(horizontal='left',vertical="center"),
#                             border=Border(left=Side(border_style='thin',
#                                                             color='FF000000'),
#                                           right=Side(border_style='thin',
#                                                      color='FF000000'),
#                                           # top=Side(border_style='thin',
#                                                    # color='FF000000'),
#                                           bottom=Side(border_style='thin',
#                                                       color='FF000000')))
#             s_rate = Style(alignment=Alignment(horizontal='right',vertical="center"),
#                             border=Border(left=Side(border_style='thin',
#                                                         color='FF000000'),
#                                           right=Side(border_style='thin',
#                                                      color='FF000000'),
#                                           bottom=Side(border_style='dashed',
#                                                       color='FF000000')))
#             s_rate1 = Style(alignment=Alignment(horizontal='right',vertical="center"),
#                             border=Border(left=Side(border_style='thin',
#                                                         color='FF000000'),
#                                           right=Side(border_style='thin',
#                                                      color='FF000000'),
#                                           bottom=Side(border_style='thin',
#                                                       color='FF000000')))

#             for i in xrange(0,cnt):
#                 ws.cell('A%d' % (indx+i)).style = Style(alignment=Alignment(vertical="center",horizontal='center'),
#                                                     border=Border(left=Side(border_style='thin',
#                                                                             color='FF000000'),
#                                                                   right=Side(border_style='thin',
#                                                                              color='FF000000'),
#                                                                   # top=Side(border_style='thin',
#                                                                   #          color='FF000000'),
#                                                                   bottom=Side(border_style='thin',
#                                                                               color='FF000000')))

#                 ws.cell('B%d' % (indx+i)).style = Style(alignment=Alignment(vertical="center",horizontal='left'),
#                                                     border=Border(left=Side(border_style='thin',
#                                                                             color='FF000000'),
#                                                                   right=Side(border_style='thin',
#                                                                              color='FF000000'),
#                                                                   # top=Side(border_style='thin',
#                                                                   #          color='FF000000'),
#                                                                   bottom=Side(border_style='thin',
#                                                                               color='FF000000')))

#                 ws['C%d' % (indx+i)] = info['equip_info'][i]['dtype']
#                 ws.cell('C%d' % (indx+i)).style = style if i<(cnt-1) else style1

#                 ws['D%d' % (indx+i)] = info['equip_info'][i]['algo']
#                 ws.cell('D%d' % (indx+i)).style = style if i<(cnt-1) else style1

#                 ws['E%d' % (indx+i)] = info['equip_info'][i]['weight']
#                 ws.cell('E%d' % (indx+i)).style = style if i<(cnt-1) else style1

#                 ws['F%d' % (indx+i)] = info['equip_info'][i]['thresh']
#                 ws.cell('F%d' % (indx+i)).style = s_rate if i<(cnt-1) else s_rate1

#                 ws.row_dimensions[indx+i].height = 24.0

#             ldirid_no += 1
#             indx += cnt

#         # start, stop = 2, indx-1
#         # for index,row in enumerate(ws.iter_rows()):
#         #     if start < index < stop:
#         #         for cell in row:
#         #             cell.style = None

#         for i,cname in enumerate(cols):
#             self.logger.info('%s, %s: %d' % (get_column_letter(i+1), cname, cols_info[cname]))
#             ws.column_dimensions[get_column_letter(i+1)].width = cols_info[cname]

#         # 应用样式
#         # ws.row_dimensions[1].style = s_head

#         wb.save(fname)
#         self.logger.info('write %s successfully.' % (fname))

#     def queryLinkDirJudgeInfo1(self):
#         cursor = self.conn['tfm'].cursor()

#         qry = """
#           with
#           conf as (select t.*,t1.type,t1.detectorid,t1.detectcode
#                     from
#                     (select t2.linkdirid,t2.linkdirname,t.drivewayid
#                      from t_tfm_link_dir_in_out t right join t_tfm_link_dir t2 on t.linkdirid=t2.linkdirid) t
#                     left join
#                     t_tfm_drive_and_channel t1
#                     on t1.driveid=t.drivewayid),
#           res as (select t.linkdirid,t.type,t.detectorid,count(0) lane_cnt
#                   from conf t
#                   group by t.linkdirid,t.type,t.detectorid)
#           select t2.roadid,
#                  t2.roadname,
#                  t.linkdirid,
#                  (select linkdirname from t_tfm_link_dir  where linkdirid=t.linkdirid) lname,
#                  count(distinct t.type) over(partition by t.linkdirid) device_cnt,
#                  t.type,t.detectorid,t.lane_cnt
#           from res t
#           left join
#           (select t.linkdirid,t.linkdirname,t1.roadid,t1.roadname from t_tfm_link_dir t
#             left join
#             (select t.id,t.length,t.roadid,t.roadname from t_tfm_link t) t1
#             on t.linkid=t1.id) t2
#           on t.linkdirid=t2.linkdirid
#           order by t2.roadname,t.linkdirid,t.type
#         """

#         self.logger.info(qry)

#         cursor.execute(qry)

#         result = OrderedDict()
#         for row in common.toDict(cursor):
#             if not result.has_key(row['roadid']):
#                 ldir = {}
#                 ldir[row['linkdirid']] = {
#                     'lname': row['lname'],
#                     'cnt': 1,
#                     'device_cnt': row['device_cnt'],
#                     'device_info': [{
#                         'dtype': row['type'],
#                         'did': row['detectorid'],
#                         'lane_cnt': row['lane_cnt'],
#                     }],
#                 }

#                 result[row['roadid']] = {
#                     'rname': row['roadname'],
#                     'linkdirs': ldir,
#                     'cnt': 1,
#                 }
#             else:
#                 road_info = result[row['roadid']]
#                 if not road_info['linkdirs'].has_key(row['linkdirid']):
#                     road_info['linkdirs'][row['linkdirid']] = {
#                         'lname': row['lname'],
#                         'cnt': 1,
#                         'device_cnt': row['device_cnt'],
#                         'device_info': [{
#                             'dtype': row['type'],
#                             'did': row['detectorid'],
#                             'lane_cnt': row['lane_cnt'],
#                         }],
#                     }
#                 else:
#                     info = {
#                         'dtype': row['type'],
#                         'did': row['detectorid'],
#                         'lane_cnt': row['lane_cnt'],
#                     }
#                     road_info['linkdirs'][row['linkdirid']]['device_info'].append(info)
#                     road_info['linkdirs'][row['linkdirid']]['cnt'] += 1
#                 road_info['cnt'] += 1

#         self.logger.info(u'共获取到 %d 条数据，%d 条道路.' % (cursor.rowcount, len(result)))

#         cursor.close()

#         return result

#     def queryLinkDirJudgeInfo(self):

#         cursor = self.conn['tfm'].cursor()

#         # qry = u"""
#         #     select t2.linkdirid,
#         #          t2.linkdirname lname,
#         #          t1.equiptype,
#         #          decode(t1.equiptype,
#         #                  'UTCDETECTOR','路口地磁',
#         #                  'TGS','电子警察',
#         #                  'RFID','RFID',
#         #                  'IID','视频检测器',
#         #                  'COIL',decode(
#         #                          sign((select count(0) from (
#         #                              select * from (
#         #                               select t1.linkdirid,t2.driveid,t2.type,t2.detectcode,t2.detectorid
#         #                               from  t_tfm_link_dir_in_out t1 left join t_tfm_drive_and_channel t2
#         #                               on t1.drivewayid=t2.driveid
#         #                             ) where type='COIL' and regexp_like(detectcode,'^[NESW].*')
#         #                          ) t where t.linkdirid=t2.linkdirid)),0,'反溢地磁',1,'路段地磁'),
#         #                   '其他') dtype,
#         #        --  t1.arithmeticid,
#         #          decode(t1.arithmeticid,
#         #                   '1','饱和度',
#         #                   '13','旅行时间/流量',
#         #                   '7','历史经验值',
#         #                   '12','时间占有率') algo,
#         #          t1.rate||'%' weight
#         #   from t_tfm_linkdir_arithmetic_rate t1
#         #   right join
#         #   (select linkdirid,linkdirname from t_tfm_link_dir_focus) t2
#         #   on t1.linkdirid=t2.linkdirid
#         #   order by lname
#         # """
#         # qry = u"""
#         #   select t1.linkdirid,
#         #          (select linkdirname from t_tfm_link_dir where linkdirid=t1.linkdirid) lname,
#         #          t1.dtype equiptype,
#         #          decode(t1.dtype,
#         #                  'UTCDETECTOR','路口地磁',
#         #                  'TGS','电子警察',
#         #                  'RFID','RFID',
#         #                  'IID','视频检测器',
#         #                  'COIL',decode(
#         #                          sign((select count(0) from (
#         #                              select * from (
#         #                               select t1.linkdirid,t2.driveid,t2.type,t2.detectcode,t2.detectorid
#         #                               from  t_tfm_link_dir_in_out t1 left join t_tfm_drive_and_channel t2
#         #                               on t1.drivewayid=t2.driveid
#         #                             ) where type='COIL' and regexp_like(detectcode,'^[NEWS].*')
#         #                          ) t where t.linkdirid=t1.linkdirid)),0,'反溢地磁',1,'路段地磁'),
#         #                   '其他') dtype,
#         #          decode(t1.arithmeticid,
#         #                   '1','饱和度',
#         #                   '13','旅行时间/流量',
#         #                   '7','历史经验值',
#         #                   '12','时间占有率','流量通行能力比') algo,
#         #          nvl2(t1.rate, (t1.rate||'%'), '0%') weight,
#         #          decode(t1.arithmeticid,
#         #                 '1', (select to_char(t.saturationlower,'0.99')||' - '||to_char(t.saturationupper,'0.99') from t_tfm_link_dir t where t1.linkdirid=linkdirid),
#         #                 '13',(select t.traveltimellimit||' - '||t.traveltimeulimit from t_tfm_link_dir t where t1.linkdirid=linkdirid),
#         #                 '12',(select to_char(t.timeopyl,'99.9')||' - '||to_char(t.timeopyu,'99.9') from t_tfm_link_dir t where t1.linkdirid=linkdirid),
#         #                 (select to_char(t.saturationlower,'0.99')||' - '||to_char(t.saturationupper,'0.99') from t_tfm_link_dir t where t1.linkdirid=linkdirid)) thresh
#         #   from (
#         #     select t.*,
#         #            t1.arithmeticid,
#         #            t1.rate
#         #     from (
#         #       select t.*
#         #       from (
#         #         select t1.linkdirid,
#         #                t2.type dtype
#         #         from (
#         #         select linkdirid,drivewayid from t_tfm_link_dir_in_out
#         #         where linkdirid in (select linkdirid from t_tfm_link_dir_focus)
#         #         ) t1
#         #         left join
#         #         t_tfm_drive_and_channel t2
#         #         on t1.drivewayid=t2.driveid
#         #         group by t1.linkdirid,t2.type
#         #       ) t
#         #       union
#         #       (
#         #       select t.linkdirid,t.equiptype dtype from t_tfm_linkdir_arithmetic_rate t
#         #       where t.linkdirid in (select linkdirid from t_tfm_link_dir_focus)
#         #       )
#         #     ) t left join t_tfm_linkdir_arithmetic_rate t1
#         #     on t.dtype=t1.equiptype and t.linkdirid=t1.linkdirid
#         #   ) t1
#         #   order by t1.linkdirid
#         # """

#         qry = """
#           with
#           conf as (select t.*,t1.type,t1.detectorid,t1.detectcode
#                     from
#                     (select t2.linkdirid,t2.linkdirname,t.drivewayid
#                      from t_tfm_link_dir_in_out t right join t_tfm_link_dir t2 on t.linkdirid=t2.linkdirid) t
#                     left join
#                     t_tfm_drive_and_channel t1
#                     on t1.driveid=t.drivewayid),
#           res as (select t.linkdirid,t.type,t.detectorid,count(0) lane_cnt
#                   from conf t
#                   group by t.linkdirid,t.type,t.detectorid)
#           select t2.roadname,
#                  t.linkdirid,
#                  (select linkdirname from t_tfm_link_dir  where linkdirid=t.linkdirid) lname,
#                  count(distinct t.type) over(partition by t.linkdirid) device_cnt,
#                  t.type,t.detectorid,t.lane_cnt
#           from res t
#           left join
#           (select t.linkdirid,t.linkdirname,t1.roadid,t1.roadname from t_tfm_link_dir t
#             left join
#             (select t.id,t.length,t.roadid,t.roadname from t_tfm_link t) t1
#             on t.linkid=t1.id) t2
#           on t.linkdirid=t2.linkdirid
#           order by t2.roadname,t.linkdirid,t.type
#         """

#         self.logger.info(qry)

#         cursor.execute(qry)

#         result = OrderedDict()
#         for row in common.toDict(cursor):
#             if not result.has_key(row['linkdirid']):
#                 result[row['linkdirid']] = {
#                     'lname': row['lname'],
#                     'equip_info': [
#                         {
#                             'equiptype': row['equiptype'],
#                             'dtype': row['dtype'],
#                             'algo': row['algo'] if row['algo'] is not None else None,
#                             'weight': row['weight'],
#                             'thresh': row['thresh'],
#                         }
#                     ],
#                     'equip_cnt': 2,
#                 }
#             else:
#                 equip_info = {
#                     'equiptype': row['equiptype'],
#                     'dtype': row['dtype'],
#                     'algo': row['algo'] if row['algo'] is not None else None,
#                     'weight': row['weight'],
#                     'thresh': row['thresh'],
#                 }
#                 result[row['linkdirid']]['equip_info'].append(equip_info)
#                 result[row['linkdirid']]['equip_cnt'] += 1

#         for ldirid, info in result.iteritems():
#             info['equip_info'].append({
#                 'equiptype': None,
#                 'dtype': None,
#                 'algo': u'历史经验值',
#                 'weight': None,
#                 'thresh': None,
#               })

#         self.logger.info(u'共获取到 %d 条数据，%d 条连线方向.' % (cursor.rowcount, len(result)))

#         cursor.close()

#         return result


# if __name__ ==  '__main__':
#     now = datetime.now()
#     tod = now.strftime('%Y-%m-%d')

#     helper = EhlHelper('Helper')
#     xlsx = u'厦门试点路段判态依据_%s.xlsx' % (tod)

#     helper.writeLinkDirEquip1(xlsx, helper.queryLinkDirJudgeInfo1())
