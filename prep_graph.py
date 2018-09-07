

import pandas as pd

from neo4j.v1 import GraphDatabase

import util as util

class myNeoGraph(object):

    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self._driver.close()
     
    @staticmethod
    def add_callend(tx):
          result = tx.run(
                         "CREATE (s:CallEnd { id: 'CallEnd'}) "
                       )
      
    @staticmethod
    def add_callend_map(tx, menuid, optionid):
          result = tx.run(
                         "MATCH (m:MenuNode { menuid: $menuid}), (ce:CallEnd { id: 'CallEnd'}) "
                         "CREATE (m)-[r_m_ce:Disconnect {optionid: $optionid, n: 1}]->(ce) RETURN r_m_ce "
                         ,menuid = menuid
                         ,optionid = optionid
                       )
          
    @staticmethod
    def upd_callend_map(tx, menuid, optionid):
          result = tx.run(
                         "MATCH (m:MenuNode { menuid: $menuid})-[r:Disconnect]->(ce:CallEnd { id: 'CallEnd'}) "
                         "SET r = {optionid: $optionid, n: r.n+1} " 
                         "RETURN r.optionid AS optionid, r.n as n"
                         ,menuid = menuid
                         ,optionid = optionid
                       )
          
          return result
         
    @staticmethod
    def get_callend_map(tx, menuid, optionid):
          result = tx.run(
                         "MATCH (m:MenuNode { menuid: $menuid})-[r:Disconnect]->(ce:CallEnd { id: 'CallEnd'}) "
                         "RETURN r.optionid AS optionid, r.n as n"
                         ,menuid = menuid
                       )
          
          res = []
          res_dict = {}
          res_found_flag = False
          if result:
            #if len(result):
            #  print("##Error!!! Multiple records found for realtionship menu_to_menu. This needs further fix...".format(len(result)))
            for resultset in result:
              #if res_found_flag == False:
              res_dict = {}
              if resultset['optionid'] == optionid:
                res_found_flag = True
                res_dict['optionid'] = resultset['optionid']
                res_dict['n'] = resultset['n']
                res.append(res_dict)
                #print("##Match found c -> optionid[{}] n[{}]".format(resultset['optionid'],resultset['n']))
                break
              #print("s -> [",resultset['s'],"] ct -> [",resultset['ct'])
              #print("c -> ",resultset['r'])
             
            if res_found_flag == False:
              res = None 
          else:
            res = None 
           
          return res
      
    @staticmethod
    def add_calltype(tx, calltype):
          result = tx.run(
                         "CREATE (s:Call { id: $calltype, calltype: $calltype}) RETURN s.id "
                         ,calltype = calltype
                       )
     
    @staticmethod
    def get_calltype(tx, calltype):
          result = tx.run(
                         "MATCH (s:Call { calltype: $calltype}) RETURN s.calltype "
                         ,calltype = calltype
                       )
          res = []
          res_found_flag = False
          if result:
            for resultset in result:
              if res_found_flag == False:
                res_found_flag = True
              res.append(resultset['s.calltype'])
              #print(resultset['s.calltype'])
             
            if res_found_flag == False:
              res = None 
          else:
            res = None 
           
          return res
           
          return result
          
    @staticmethod
    def add_subscriber(tx, subscriber_id):
          result = tx.run(
                         "CREATE (s:Subscriber { id: $subscriber_id}) RETURN s.id "
                         ,subscriber_id = subscriber_id
                       )
          
    @staticmethod
    def get_subscriber(tx, subscriber_id):
          result = tx.run(
                         "MATCH (s:Subscriber { id: $subscriber_id}) RETURN s.id "
                         ,subscriber_id = subscriber_id
                       )
          
          res = []
          res_found_flag = False
          if result:
            for resultset in result:
              if res_found_flag == False:
                res_found_flag = True
              res.append(resultset['s.id'])
              #print(resultset['s.id'])
             
            if res_found_flag == False:
              res = None 
          else:
            res = None 
           
          return res
          
    @staticmethod
    def add_subscriber_to_calltype_map(tx, subscriber_id, calltype, callid):
          '''
          MATCH(s:Subscriber {id: "199"}), (c:call {calltype: "E"}) CREATE (s)-[:CALLS {callid: '1234', option: 'none'}]->(c)
          '''
          result = tx.run(
                         "MATCH (s:Subscriber { id: $subscriber_id}), (c:Call { id: $calltype}) "
                         "CREATE (s)-[r_s_ct:CALLS {callid: $callid, n: 1}]->(c) RETURN r_s_ct "
                         ,subscriber_id = subscriber_id
                         ,calltype = calltype
                         ,callid = callid
                       )
          
          res = []
          if result:
            #print(result)
            #print(result.summary())
            #print(len(result.summary()))
            for resultset in result:
              res.append(resultset['r_s_ct'])
              #print(resultset['r_s_ct'])
          else:
            res = None 
           
          return res
         
    @staticmethod
    def get_subscriber_to_calltype_map(tx, subscriber_id, calltype, callid):
          '''
       MATCH(s:Subscriber {id: "199"}), (c:call {calltype: "E"}) CREATE (s)-[:CALLS {callid: '1234', option: 'none'}]->(c)
       MATCH (s:Subscriber {id: '1150387668'})-[r:CALLS]->(c:Call {calltype:'E'}) SET r = {callid:'1150387667',n:2} RETURN r
       CREATE CONSTRAINT ON (p:Person) ASSERT p.person_id IS UNIQUE
       CREATE INDEX ON :Person(person_id);
<Relationship id=117 nodes=(<Node id=109 labels=set() properties={}>, <Node id=111 labels=set() properties={}>) type='CALLS' properties={'callid': 9152275945, 'n': 1}>
        MATCH (n) DETACH DELETE n
          '''
          result = tx.run(
                         "MATCH (s:Subscriber {id: $subscriber_id})-[r:CALLS]->(c:Call {calltype: $calltype}) "
                         #"SET r = {callid:'1150387667',n:2} " 
                         "RETURN r.callid AS callid, r.n as n"
                         ,subscriber_id = subscriber_id
                         ,calltype = calltype
                         ,callid = callid
                       )
          
          res = []
          res_dict = {}
          res_found_flag = False
          if result:
            for resultset in result:
              #if res_found_flag == False:
              res_dict = {}
              if resultset['callid'] == callid:
                res_found_flag = True
                res_dict['callid'] = resultset['callid']
                res_dict['n'] = resultset['n']
                res.append(res_dict)
                #print("##Match found c -> callid[{}] n[{}]".format(resultset['callid'],resultset['n']))
                break
              #print("s -> [",resultset['s'],"] ct -> [",resultset['ct'])
              #print("c -> ",resultset['r'])
             
            if res_found_flag == False:
              res = None 
          else:
            res = None 
           
          return res
         
    @staticmethod
    def add_subscriber_to_menu_map(tx, subscriber_id, menuid, callid):
          #print("##*******s[{}]*m[{}]*c[{}]".format(subscriber_id,menuid,callid))
          result = tx.run(
                         "MATCH (s:Subscriber { id: $subscriber_id}), (m:MenuNode { menuid: $menuid}) "
                         "CREATE (s)-[r_s_m:CALLS {callid: $callid, n: 1}]->(m) RETURN r_s_m "
                         ,subscriber_id = subscriber_id
                         ,menuid = menuid
                         ,callid = callid
                       )
          
          res = []
          if result:
            #print(result)
            #print(result.summary())
            #print(len(result.summary()))
            for resultset in result:
              res.append(resultset['r_s_m'])
              #print(resultset['r_s_m'])
          else:
            res = None 
           
          return res
         
    @staticmethod
    def get_subscriber_to_menu_map(tx, subscriber_id, menuid, callid):
          '''
       MATCH(s:Subscriber {id: "199"}), (c:call {menuid: "E"}) CREATE (s)-[:CALLS {callid: '1234', option: 'none'}]->(c)
       MATCH (s:Subscriber {id: '1150387668'})-[r:CALLS]->(m:MenuNode {menuid:'E'}) SET r = {callid:'1150387667',n:2} RETURN r
       CREATE CONSTRAINT ON (p:Person) ASSERT p.person_id IS UNIQUE
       CREATE INDEX ON :Person(person_id);
<Relationship id=117 nodes=(<Node id=109 labels=set() properties={}>, <Node id=111 labels=set() properties={}>) type='CALLS' properties={'callid': 9152275945, 'n': 1}>
        MATCH (n) DETACH DELETE n
          '''
          result = tx.run(
                         "MATCH (s:Subscriber {id: $subscriber_id})-[r:CALLS]->(m:MenuNode {menuid: $menuid}) "
                         #"SET r = {callid:'1150387667',n:2} " 
                         "RETURN r.callid AS callid, r.n as n"
                         ,subscriber_id = subscriber_id
                         ,menuid = menuid
                         ,callid = callid
                       )
          
          res = []
          res_dict = {}
          res_found_flag = False
          if result:
            for resultset in result:
              #if res_found_flag == False:
              res_dict = {}
              if resultset['callid'] == callid:
                res_found_flag = True
                res_dict['callid'] = resultset['callid']
                res_dict['n'] = resultset['n']
                res.append(res_dict)
                #print("##Match found c -> callid[{}] n[{}]".format(resultset['callid'],resultset['n']))
                break
              #print("s -> [",resultset['s'],"] ct -> [",resultset['ct'])
              #print("c -> ",resultset['r'])
             
            if res_found_flag == False:
              res = None 
          else:
            res = None 
           
          return res
         
    @staticmethod
    def add_menu_node(tx, menuid,time_spent,menu_desc):
          result = tx.run(
                         "CREATE (s:MenuNode { menuid: $menuid, menu_desc: $menu_desc, time_spent: $time_spent, n: 1}) RETURN s.id "
                         ,menuid = menuid
                         ,menu_desc = menu_desc
                         ,time_spent = time_spent
                       )
          
    @staticmethod
    def get_menu_node(tx, menuid):
          result = tx.run(
                         "MATCH (s:MenuNode { menuid: $menuid}) RETURN s.id as id, s.n as n "
                         ,menuid = menuid
                       )
          
          res = []
          res_found_flag = False
          if result:
            for resultset in result:
              res_dict = {}
              if res_found_flag == False:
                res_found_flag = True
              res_dict['id'] = resultset['id']
              res_dict['n'] = resultset['n']
              res.append(res_dict)
              #print(resultset['id'])
             
            if res_found_flag == False:
              res = None 
          else:
            res = None 
           
          return res
      
    @staticmethod
    def add_menu_to_menu_map(tx, prev_menuid, menuid, optionid):
          '''
          MATCH(s:Subscriber {id: "199"}), (c:call {calltype: "E"}) CREATE (s)-[:CALLS {callid: '1234', option: 'none'}]->(c)
          '''
          result = tx.run(
                         "MATCH (pm:MenuNode { menuid: $prev_menuid}), (m:MenuNode { menuid: $menuid}) "
                         "CREATE (pm)-[r_m_m:OptionId {optionid: $optionid, n: 1}]->(m) RETURN r_m_m "
                         ,prev_menuid = prev_menuid
                         ,menuid = menuid
                         ,optionid = optionid
                       )
          
          res = []
          if result:
            #print(result)
            #print(result.summary())
            #print(len(result.summary()))
            for resultset in result:
              res.append(resultset['r_m_m'])
              #print(resultset['r_m_m'])
          else:
            res = None 
           
          return res
         
    @staticmethod
    def upd_menu_to_menu_map(tx, prev_menuid, menuid, optionid):
          '''
<Relationship id=117 nodes=(<Node id=109 labels=set() properties={}>, <Node id=111 labels=set() properties={}>) type='CALLS' properties={'callid': 9152275945, 'n': 1}>
          '''
          result = tx.run(
                         "MATCH (pm:MenuNode { menuid: $prev_menuid})-[r:OptionId {optionid: $optionid}]->(m:MenuNode { menuid: $menuid}) "
                         "SET r = {optionid: $optionid, n: r.n+1} " 
                         #"WHERE r.optionid == $optionid "
                         "RETURN r.optionid AS optionid, r.n as n"
                         ,prev_menuid = prev_menuid
                         ,menuid = menuid
                         ,optionid = optionid
                       )
          
          return result
         
    @staticmethod
    def get_menu_to_menu_map(tx, prev_menuid, menuid, optionid):
          '''
<Relationship id=117 nodes=(<Node id=109 labels=set() properties={}>, <Node id=111 labels=set() properties={}>) type='CALLS' properties={'callid': 9152275945, 'n': 1}>
          '''
          result = tx.run(
                         #"MATCH (s:Subscriber {id: $subscriber_id})-[r:CALLS]->(c:Call {calltype: $calltype}) "
                         "MATCH (pm:MenuNode { menuid: $prev_menuid})-[r:OptionId]->(m:MenuNode { menuid: $menuid}) "
                         #"SET r = {callid:'1150387667',n:2} " 
                         "RETURN r.optionid AS optionid, r.n as n"
                         ,prev_menuid = prev_menuid
                         ,menuid = menuid
                       )
          
          res = []
          res_dict = {}
          res_found_flag = False
          if result:
            #if len(result):
            #  print("##Error!!! Multiple records found for realtionship menu_to_menu. This needs further fix...".format(len(result)))
            for resultset in result:
              #if res_found_flag == False:
              res_dict = {}
              if resultset['optionid'] == optionid:
                res_found_flag = True
                res_dict['optionid'] = resultset['optionid']
                res_dict['n'] = resultset['n']
                res.append(res_dict)
                #print("##Match found c -> optionid[{}] n[{}]".format(resultset['optionid'],resultset['n']))
                break
              #print("s -> [",resultset['s'],"] ct -> [",resultset['ct'])
              #print("c -> ",resultset['r'])
             
            if res_found_flag == False:
              res = None 
          else:
            res = None 
           
          return res
      
    @staticmethod
    def add_calltype_to_menu_map(tx, calltype, menuid, optionid):
          '''
          MATCH(s:Subscriber {id: "199"}), (c:call {calltype: "E"}) CREATE (s)-[:CALLS {callid: '1234', option: 'none'}]->(c)
          '''
          result = tx.run(
                         "MATCH (ct:Call { calltype: $calltype}), (m:MenuNode { menuid: $menuid}) "
                         "CREATE (ct)-[r_ct_m:OptionId {optionid: $optionid, n: 1}]->(m) RETURN r_ct_m "
                         ,calltype = calltype
                         ,menuid = menuid
                         ,optionid = optionid
                       )
          
          res = []
          if result:
            #print(result)
            #print(result.summary())
            #print(len(result.summary()))
            for resultset in result:
              res.append(resultset['r_ct_m'])
              #print(resultset['r_ct_m'])
          else:
            res = None 
           
          return res
         
    @staticmethod
    def upd_calltype_to_menu_map(tx, calltype, menuid, optionid):
          '''
<Relationship id=117 nodes=(<Node id=109 labels=set() properties={}>, <Node id=111 labels=set() properties={}>) type='CALLS' properties={'callid': 9152275945, 'n': 1}>
          '''
          result = tx.run(
                         "MATCH (ct:Call { calltype: $calltype})-[r:OptionId]->(m:MenuNode { menuid: $menuid}) "
                         "SET r = {optionid: $optionid, n: r.n+1} " 
                         "RETURN r.optionid AS optionid, r.n as n"
                         ,calltype = calltype
                         ,menuid = menuid
                         ,optionid = optionid
                       )
          
          return result
         
    @staticmethod
    def get_calltype_to_menu_map(tx, calltype, menuid, optionid):
          '''
<Relationship id=117 nodes=(<Node id=109 labels=set() properties={}>, <Node id=111 labels=set() properties={}>) type='CALLS' properties={'callid': 9152275945, 'n': 1}>
          '''
          result = tx.run(
                         #"MATCH (s:Subscriber {id: $subscriber_id})-[r:CALLS]->(c:Call {calltype: $calltype}) "
                         "MATCH (ct:Call { calltype: $calltype})-[r:OptionId]->(m:MenuNode { menuid: $menuid}) "
                         #"SET r = {callid:'1150387667',n:2} " 
                         "RETURN r.optionid AS optionid, r.n as n"
                         ,calltype = calltype
                         ,menuid = menuid
                       )
          
          res = []
          res_dict = {}
          res_found_flag = False
          if result:
            #if len(result):
            #  print("##Error!!! Multiple records found for realtionship menu_to_menu. This needs further fix...".format(len(result)))
            for resultset in result:
              #if res_found_flag == False:
              res_dict = {}
              if resultset['optionid'] == optionid:
                res_found_flag = True
                res_dict['optionid'] = resultset['optionid']
                res_dict['n'] = resultset['n']
                res.append(res_dict)
                #print("##Match found c -> optionid[{}] n[{}]".format(resultset['optionid'],resultset['n']))
                break
              #print("s -> [",resultset['s'],"] ct -> [",resultset['ct'])
              #print("c -> ",resultset['r'])
             
            if res_found_flag == False:
              res = None 
          else:
            res = None 
           
          return res
      
    def process_df(self, cj_df):
      with self._driver.session() as session:
        #print(greeting)
         
        prev_subscriber_id = None 
        prev_menuid = None 
        prev_callid = None 
        prev_optionid = None 
        time_spent = 0 
        tot_time_spent = 0 
        subscriber_change_flag = False
        cnt = 0
        for i,rec in cj_df.iterrows():
          ''' 
          if cnt == 0:
            prev_subscriber_id = rec.subscriber_id
            prev_menuid = rec.menuid
            prev_callid = rec.callid
            prev_optionid = rec.optionid
          if cnt > 50:
            break
          ''' 
           
          #put the only one callend node  
          if cnt == 0:
            session.write_transaction( self.add_callend)
           
          #process subscriber change or handle new call
          if prev_subscriber_id != rec.subscriber_id:
            subscriber_change_flag = True
             
            print("##inserting [{}] [{}] [{}]".format(str(rec.subscriber_id), str(rec.callid), rec.calltype))    
             
            #process code for inserting end call for previous call.
            res = session.read_transaction( self.get_menu_node, prev_menuid)
            if res:
              res = session.read_transaction( self.get_callend_map, prev_menuid, prev_optionid)
              if res:
                print("##---->Updating callend meuniid[{}]".format(prev_menuid))
                res = session.write_transaction( self.upd_callend_map, prev_menuid, prev_optionid)
              else:
                print("##---->Adding callend meuniid[{}]".format(prev_menuid))
                res = session.write_transaction( self.add_callend_map, prev_menuid, prev_optionid)
            
            #initialize key variables when new subscriber record
            tot_time_spent = 0
            prev_menuid = rec.menuid
            prev_optionid = rec.optionid 
            
            res = session.read_transaction( self.get_subscriber, str(rec.subscriber_id))
            if res == None:
              #print("##writing new rec as no rec for subscriber_id[{}]".format(rec.subscriber_id))
              session.write_transaction( self.add_subscriber, str(rec.subscriber_id))
            #else:
              #print("##[{}] rec's found for subscriber_id[{}]".format(len(res),rec.subscriber_id))
             
            '''  #used if calltype maps to menu
            res = session.read_transaction( self.get_calltype, str(rec.calltype))
            if res == None:
              #print("##writing new rec as no rec for calltype[{}]".format(rec.calltype))
              session.write_transaction( self.add_calltype, rec.calltype)
            #else:
              #print("##[{}] rec's found for subscriber_id[{}]".format(len(res),rec.calltype))
             
            res = session.read_transaction( self.get_subscriber_to_calltype_map, rec.subscriber_id, rec.calltype, rec.callid)
            if res == None:
              #print(res)
              #print("##adding map for s[{}] ct[{}] id[{}]".format(str(rec.subscriber_id), str(rec.calltype), str(rec.callid)))    
              res = session.write_transaction( self.add_subscriber_to_calltype_map, rec.subscriber_id, rec.calltype, rec.callid)
            #else:
              #print("##map already exists for s[{}] ct[{}] id[{}]".format(str(rec.subscriber_id), str(rec.calltype), str(rec.callid)))    
            ''' 
             
            #check if menu node exist, if not then create one and then map 
            time_spent = (util.get_sec(rec.end_ts) - util.get_sec(rec.start_ts))
            res = session.read_transaction( self.get_menu_node, rec.menuid)
            if res == None:
              print("##---->Adding menu meunid[{}]".format(rec.menuid))
              res = session.write_transaction( self.add_menu_node, rec.menuid, time_spent, rec.menu_desc)
           
            '''  #not required since calltype is not used here..instead subscriber is map directly to menu
            #check if calltype to menu relationship exist, if not then create new else update frequency for existing.
            res = session.read_transaction( self.get_subscriber_to_menu_map, rec.calltype, rec.menuid, prev_optionid)
            if res == None:
              print("##---->Adding sub to menu map[{}]*menuid[{}]*optionid[{}]".format(rec.calltype, rec.menuid, prev_optionid)) 
              res = session.write_transaction( self.add_subscribercalltype_to_menu_map, rec.calltype, rec.menuid, prev_optionid)
            else:
              print("##---->Updating sub to menu map[{}]*menuid[{}]*optionid[{}]".format(rec.subscriber_id, rec.menuid, prev_optionid)) 
              res = session.write_transaction( self.upd_subscribercalltype_to_menu_map, rec.calltype, rec.menuid, prev_optionid)
            ''' 
            
            #Maps Subscriber directly to menu 
            res = session.read_transaction( self.get_subscriber_to_menu_map, rec.subscriber_id, rec.menuid, rec.callid)
            if res == None:
              #print(res)
              print("##-->***adding sub to menu map s[{}] m[{}] callid[{}]".format(str(rec.subscriber_id), str(rec.menuid), str(rec.callid)))    
              res = session.write_transaction( self.add_subscriber_to_menu_map, rec.subscriber_id, rec.menuid, rec.callid)
            #else: #code pending if same subscriber call for same reason
              #print("##map already exists for s[{}] ct[{}] id[{}]".format(str(rec.subscriber_id), str(rec.menuid), str(rec.callid)))    
            
          time_spent = (util.get_sec(rec.end_ts) - util.get_sec(rec.start_ts))
          tot_time_spent += time_spent
             
          #if prev_menuid != rec.menuid:
          print("##-->menuid[{}]*optionid[{}]*time_spent[{}]*start_ts[{}]*end_ts[{}]*desc[{}]".format(str(rec.menuid), str(prev_optionid), time_spent, rec.start_ts, rec.end_ts, rec.menu_desc))    
           
          #check if menu node exist, if not then create one and then map 
          res = session.read_transaction( self.get_menu_node, rec.menuid)
          if res == None:
            res = session.write_transaction( self.add_menu_node, rec.menuid, time_spent, rec.menu_desc)
            res = session.write_transaction( self.add_menu_to_menu_map, prev_menuid, rec.menuid, prev_optionid)
           
          #check if menu to menu relationship exist, if not then create new else update frequency for existing.
          res = session.read_transaction( self.get_menu_to_menu_map, prev_menuid, rec.menuid, prev_optionid)
          if res == None:
            print("##---->Adding menu map menuid[{}]*menuid[{}]*optionid[{}]".format(prev_menuid, rec.menuid, prev_optionid)) 
            res = session.write_transaction( self.add_menu_to_menu_map, prev_menuid, rec.menuid, prev_optionid)
          else:
            print("##---->updating menu map menuid[{}]*menuid[{}]*optionid[{}]".format(prev_menuid, rec.menuid, prev_optionid)) 
            res = session.write_transaction( self.upd_menu_to_menu_map, prev_menuid, rec.menuid, prev_optionid)
          '''
          else:
            print("##---->menuid[{}]*optionid[{}]*time_spent[{}]*start_ts[{}]*end_ts[{}]*desc[{}]".format(str(rec.menuid), str(prev_optionid), time_spent, rec.start_ts, rec.end_ts, rec.menu_desc))    
          '''
          
          prev_subscriber_id = rec.subscriber_id
          prev_menuid = rec.menuid
          prev_callid = rec.callid
          prev_optionid = rec.optionid
          subscriber_change_flag = False
           
          cnt += 1
         
      #return result.single()[0]
     
    def load_csv(self,csv_filepath):
      print("##loading csv [{}]".format(csv_filepath))
      
      cj_df = pd.read_csv(csv_filepath)
      print("cj_df rows [{}]".format(cj_df.callid.count()))
      
      cj_df = cj_df.loc[:,['subscriber_id','callid','calltype','event_seq','appid','menuid','start_ts','end_ts','optionts','optionid','callts','menu_desc','option_desc']]
      cj_df = cj_df[cj_df.subscriber_id != "(null)"].sort_values(['subscriber_id','callid','event_seq'],ascending=True)
      print(cj_df.head(20))
      #self._process_df_for_graph(cj_df)
     
      return cj_df

if __name__ == "__main__":
  uri = 'bolt://localhost:7687'
  username = 'neo4j'
  passwd = 'temp1234$'
  csv_filepath = 'data/sprint_cust_journey.csv'
  
  gdb = myNeoGraph( uri, username, passwd)
  #gdb.print_greeting("Hello GraphDB")
  
  cj_df = gdb.load_csv(csv_filepath) 
  gdb.process_df(cj_df)

  gdb.close()
