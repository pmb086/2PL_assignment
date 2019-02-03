
'''
    Authors: Balaji Paiyur Mohan(1001576836)
'''
'''
In this project we have implemented rigorous 2PL with wait die protocol. 
Transaction table keeps track of the Transaction and its states and phases.
Each transaction is timestamped to schedule on first come first serve basis(following wait die protocol) 
Lock Table records all the data item's lock statuses and its corresponding transactions.

'''
#Reference Source:
#https://stackoverflow.com/questions/16333296/how-do-you-create-nested-dict-in-python
#https://docs.python.org/2/tutorial/inputoutput.html
#https://github.com/DDimitris/2PL/tree/master/src/dbProject
#https://github.com/shubhang-arora/two-phase-locking/blob/master/Assignment.py
#https://github.com/sadhana55?tab=repositories
import sys;

transaction_table={} #Transaction table is a dictionry of dictionaries(wherein key is the transaction id and value is the dictionary of its attributes)
lock_table={} # Lock table is a dictionary of dictionaries(wherein key is the item id and value is the dictionary of its attributes)
ts = 0	 # Timestamp is instantiated to 0(Transaction with ts = 0 is the oldest)

def read_item(line):
    operation = line[0]#r in r1 (X),
    line_transaction = line.split(" ") 
    transaction_id = line_transaction[0][1]# 1 in r1 (X) 
    item_id = line_transaction[1][1] #X in r1 (X)
    status_RL = "read_lock" 
    current_transaction = transaction_table[transaction_id]#Current Transaction id is extracted from the transaction table 

    if current_transaction['phase'] == True:# Phaseof the transaction is true when it is in growing phase(Wherein read, write , upgrade is done and the dataitems can only acquire the locks and none cane be released or downgraded)
        if item_id in lock_table: # Check if there is entry of item id in the lock table
            current_lock = lock_table[item_id]# Instantiating the current lock to the corresponding item id in the lock table
            if current_lock['lock_status'] == "read_lock":                             
                if transaction_id not in current_lock['locking_transactions']:  #if this trasactionis not available in locking transaction make an entry in the locking transactions list against the item id
                    (current_lock['locking_transactions']).append(transaction_id)      
                    current_transaction['trans_item'].append(item_id)                      
                    print("\nExecuting " + line + "\nChanges in Tables: The item " + str(item_id)+ "'s lock status for the transaction "  + str(transaction_id) + " is updated to Read lock again in the lock table")

            elif current_lock['lock_status'] == "write_lock":
                if len(current_lock['locking_transactions'])==0: # Before upgrading an item lock status from read lock to write lock we have to check if there are multiple read locks on the particular item                
                    current_lock['locking_transactions'].append(transaction_id)
                    current_lock['lock_status'] = status_RL
                    current_transaction['trans_item'].append(item_id)
                    print("\nExecuting " + line + "\nChanges in Tables: The item " + str(item_id) + "'s lock status for the transaction " + str(transaction_id) + " is downgraded from 'Write lock' to 'Read lock' in the lock table")

                elif transaction_id not in current_lock ['locking_transactions']:
                    t = current_lock['locking_transactions'][0]# If the item is not locked at all by that transaction then check for the priority of conflicting  transaction based on wait die protocol 
                    waitdie(t,transaction_id,operation,item_id,line)

        else: # if the item is not available in the lock table, then make new entry in the lock table
            lock_table[item_id] = {'lock_status': status_RL, 'locking_transactions': [transaction_id], 'list_of_transactions_waiting': []}
            transaction_table[transaction_id]['trans_item'].append(item_id)
            print("\nExecuting " + line + "\nChanges in Tables: Added item_id entry into lock table, Lock Status is updated to 'Read Lock', The item " + str(item_id) + " for transaction " + str(transaction_id) +" is newly added into lock table and also added the item to transaction table in corresponding transaction")

def write_item(line):
    operation = line[0]#W in W1 (X)
    line_transaction = line.split(" ")
    transaction_id = line_transaction[0][1]#1 in W1 (X)
    item_id = line_transaction[1][1]#X in W1 (X)
    status_WL = "write_lock"
    current_transaction = transaction_table[transaction_id]

    if transaction_table[transaction_id]['phase'] == True:# Phaseof the transaction is true when it is in growing phase(Wherein read, write , upgrade is done and the dataitems can only acquire the locks and none cane be released or downgraded)
        if item_id in lock_table:# Check if there is entry of item id in the lock table
            current_lock = lock_table[item_id]# Instantiating the current lock to the corresponding item id in the lock table
            t_list = current_lock['locking_transactions']# retrieving the list of locking transactions of the current item
            if current_lock['lock_status'] == "read_lock":                 
                if (len(t_list)==1 and t_list[0] == transaction_id):#Only one transaction can hold this item with read lock inorder to directly upgrade to the write lock
                    current_lock['lock_status'] = "write_lock"
                    print("\nExecuting " + line + "\nChanges in Tables: The item " + str(item_id) + "'s lock status for transaction "+ str(transaction_id) +" is upgraded from 'Read Lock' to 'Write Lock'")

                else: # IF the list has more than one transaction holding the read lock on current item check which transaction should be executed first
                    print("\nExecuting " + line + "\nChanges in Tables: Found multiple read locks on the item " + str(item_id) + "for this transaction " + str(transaction_id) + ". The operations on the transactions are performed based on the result of wait-die protocol.")
                    for t1 in t_list:
                        protocol = waitdie(t1,transaction_id,operation,item_id,line)#check the timestamp of the current transaction against all the transactions in the list so as to abort or terminate one among them using wait die protocol
                        if protocol == "aborted":
                            break # if the result come out to be aborted the current transaction is terminated

            elif current_lock['lock_status'] == "write_lock":
                if transaction_id not in t_list:             
                    t = t_list[0] # Since it already has write lock on the item the wait die algorithm has to be performed to know which transaction should be granted write lock
                    waitdie(t,transaction_id,operation,item_id,line)

        else: # if the item is not available in the lock table, then make new entry in the lock table
            lock_table[item_id] = {'lock_status': status_WL, 'locking_transactions': [transaction_id], 'list_of_transactions_waiting': []}
            transaction_table[transaction_id]['trans_item'].append(item_id)  
            print("\nExecuting " + line + "\nChanges in Tables: Added item_id entry into lock table, Lock Status is updated to 'Write Lock', The item " + str(item_id) + " for transaction " + str(transaction_id) +" is newly added into lock table and also added the item to transaction table in corresponding transaction")
			
def end_transaction(t,t_state,line):
    try:
        status_RL = "read_lock"
        status_WL = "write_lock"
        t_active = "active"
        t_aborted = "aborted"
        t_committed = "committed"
        current_transaction = transaction_table[t]
        current_transaction['trans_state']=t_state
        current_transaction['phase'] = False # Phase of the current transaction is set to false since it goes to shrinking phase(Wherein unlock and downgrading is done)

        for d_item in current_transaction['trans_item']:
            current_lock = lock_table[d_item]
            t_list = current_lock['locking_transactions']
            waiting_t_list = current_lock['list_of_transactions_waiting']
            t_list.remove(t)# the transaction holding the item has to be removed from locking transactions in lock table(shrinking phase)
            if len(waiting_t_list)== 0:
                continue # If there are no transaction waiting on the item then it proceeds to the next operation on the line
            first_waiting_trans = waiting_t_list.pop(0) # First transaction waiting in the queue is popped in order to put the transaction back to the active state
            trans1 = transaction_table[first_waiting_trans]
            op1 = trans1['list_of_waitingoperations'].pop(0) # Corresponding operation for the waiting transaction is being extracted

            if op1['operation'] == "r": #checking  if the waiting operartion is a read operation
                if current_lock['lock_status'] == status_WL:#checking if it has conflicting write lock
                    current_lock['lock_status'] = status_RL # Downgrading the lock status to read lock 
                    current_lock['locking_transactions'] = [first_waiting_trans]
                    trans1['trans_state'] = t_active # Updating the transaction id's state to active in the transaction table 
                    nxt_op = transaction_table[first_waiting_trans]['list_of_waitingoperations'][0] # The next waiting operation is being popped
                    if nxt_op['operation'] == "e":
                        transaction_table[first_waiting_trans]['list_of_waitingoperations'].pop(0)# Previous transaction is committed and the next opeartion will be in the queue
                        print("\n Executing e" + str(first_waiting_trans)+  " \nChanges in Tables: The Transaction " + str(first_waiting_trans)+  " is committed and the next waiting item to be unlocked: " + str(transaction_table[first_waiting_trans]['trans_item']))

                        end_transaction(first_waiting_trans,t_committed,line) # End transaction is recursively called since next operation is end_transaction
                 #Iterating through the list of waiting transactions 
                    for t in waiting_t_list:
                        t1 = transaction_table[t] # Extracts the row against transaction id
                        t2 = t1['list_of_waitingoperations'][0] # Gets the first waiting opeartion 
                        if t2['operation'] == "r":
                            t1['list_of_waitingoperations'].pop(0) # popping out the first waiting operation 
                            waiting_t_list.remove(t) # The transaction is removed from the list of waiting transaction
                            t1['trans_state'] = t_active # The popped transaction is set to active again
                            t_list.append(t)  # This transaction is being updated in the current locking transaction list
                            nxt_op = transaction_table[first_waiting_trans]['list_of_waitingoperations'][0] # popping the next waiting opeartion
                            if nxt_op['operation'] == "e":
                                transaction_table[first_waiting_trans]['list_of_waitingoperations'].pop(0)# Previous transaction is committed and the next opeartion will be in the queue
                                print("\n Executing e" + str(first_waiting_trans) + " \nChanges in Tables: The Transaction " + str(first_waiting_trans) + " is committed and the next waiting item to be unlocked: " + str(transaction_table[first_waiting_trans]['trans_item']))
                                
                                end_transaction(first_waiting_trans, t_committed, line) # End transaction is recursively called since next operation is end_transaction

            elif op1['operation'] == "w":#checking  if the waiting operartion is a write operation
                if current_lock['lock_status'] == status_RL:#checking if it has conflicting read lock
                    if len(t_list)==0 or ((len(t_list))==1 and t_list[0] == first_waiting_trans ): # since we are upgrading the current item lock from the read lock to write lock it has to be either 0 or 1 transaction holding it
                        print("\nExecuting waiting operations")
                        write_item(str(op1['operation'])+str(first_waiting_trans)+" ("+str(op1['item_id'])+");")
                        current_lock['lock_status'] = status_WL
                        t_list=[first_waiting_trans] # Gets the  first waiting transaction 
                        trans1['trans_state'] = t_active# Updating the transaction id's state to active in the transaction table 
                        nxt_op2 = transaction_table[first_waiting_trans]['list_of_waitingoperations'][0] # pops the next waiting opeartion
                        if nxt_op2['operation'] == "e":
                            transaction_table[first_waiting_trans]['list_of_waitingoperations'].pop(0)# Previous transaction is committed and the next opeartion will be in the queue
                            print("\n Executing e" + str(first_waiting_trans) + " \nChanges in Tables: The Transaction " + str(first_waiting_trans) + " is committed and the next waiting item to be unlocked: " + str(transaction_table[first_waiting_trans]['trans_item']))
                            
                            end_transaction(first_waiting_trans, t_committed, line)# End transaction is recursively called since next operation is end_transaction

                    elif (len(t_list) > 0): # If the list has more locking transactions then the next possible transaction to be performed is decided by wait die protocol
                        for t1 in t_list:
                            protocols = waitdie(t1,first_waiting_trans,op1['operation'],op1['item_id'],line)
                            if protocols == t_aborted:
                                break;

                elif current_lock['lock_status'] == status_WL:
                    t_list = [first_waiting_trans] 
                    trans1['trans_state'] = t_active # Updating the transaction id's state to active in the transaction table 
                    print("\nExecuting " + line + "\nChanges in Tables: The Transaction releases read lock on the item " +str(current_lock['item_id']))
                    
                    nxt_op2 = transaction_table[first_waiting_trans]['list_of_waitingoperations'][0]
                    if nxt_op2['operation'] == "e": # pops the next waiting opeartion
                        print("\nExecuting e"  + str(first_waiting_trans) + " \nChanges in Tables: The Transaction " + str(first_waiting_trans) + " is committed and the next waiting item to be unlocked: " + str(transaction_table[first_waiting_trans]['trans_item']))
                        
                        transaction_table[first_waiting_trans]['list_of_waitingoperations'].pop(0)
                        end_transaction(first_waiting_trans, t_committed, line) # End transaction is recursively called since next operation is end_transaction

        current_transaction['trans_item'].clear() # Since the current transaction list varies for each transaction id it is cleared on each iteration
    except IndexError as e:
        e = sys.exc_info()[0]
'''In this scheme, if a transaction requests to lock a resource (data item), which is already held with a conflicting lock by another transaction, then one of the two possibilities may occur −

If TS(Ti) < TS(Tj) − that is Ti, which is requesting a conflicting lock, is older than Tj − then Ti is allowed to wait until the data-item is available.

If TS(Ti) > TS(tj) − that is Ti is younger than Tj − then Ti dies. Ti is restarted later with a random delay but with the same time stamp.

This scheme allows the older transaction to wait but kills the younger one.'''

def waitdie(t1,t2,operation,item_id,line):
    current_lock = lock_table[item_id]
    t_list = lock_table[item_id]['locking_transactions']
    t_aborted = "aborted"
    t_blocked = "blocked"
    Trans1 = transaction_table[t1]
    Trans2= transaction_table[t2]
    TS1 = Trans1['Timestamp'] 
    TS2 = Trans2['Timestamp'] 

    if Trans1 == Trans2:
        return "Both Transactions are same"
    if Trans2['trans_state'] != t_blocked or Trans2['trans_state'] != t_aborted: # the conflicting transaction should also be active in order to implement wait die protocol
        if TS2 < TS1: # If TS2 is older than TS1, TS2 is made to wait until the data item is released
            Trans2['trans_state'] = t_blocked
            Trans2['list_of_waitingoperations'].append({'operation':operation, 'item_id':item_id})
            current_lock['list_of_transactions_waiting'].append(t2) # The TS2 is being added to the list of waiting transaction for the particular item
            print("\nExecuting " + line + "\nChanges in Tables: Transaction "+ str(t2) +" is blocked, and it's being added to list of waiting operations : " +str(Trans2['list_of_waitingoperations'])) 

        else:
            print("\nExecuting " + line + "\nChanges in Tables: Transaction "+ str(t2) + " is aborted, and the subsequent operations are disregarded") 

            end_transaction(t2,t_aborted,line)# End transaction is recursively called since the TS2 dies and restarts later with the same timestamp
	
with open("input.txt", "r") as file: #It is good practice to use the with keyword when dealing with file objects. This has the advantage that the file is properly closed after its suite finishes, even if an exception is raised on the way. It is also much shorter than writing equivalent try-finally
    for line in file: # Iterating through each line
        operation = line[0]
        # Begin Transaction
        if operation == "b" or operation == "e":
             transaction_id = line[1]
             if operation == "b":
                 t_state = "active"
                 phase = True
                 ts = ts + 1 # Since each transaction's timestamp is incremented by 1
                 transaction_table[transaction_id] = {'trans_state': t_state, 'Timestamp': ts, 'list_of_waitingoperations': [], 'trans_item' : [], 'phase': phase}
                 # New entry of transaction id is added to the transaction table
                 print("\nExecuting " +line+ "\nChanges in Tables: New transaction is added to the transaction table with Transaction ID: " + str(transaction_id))
             
             elif operation == "e":
                 if transaction_id in transaction_table:
                     if transaction_table[transaction_id]['trans_state'] == 'blocked': # Since the transaction is already blocked end_transaction cannot be performed
                         transaction_table[transaction_id]['list_of_waitingoperations'].append({'operation':operation, 'item_id' : 'N.A.'})
                         print("\nExecuting " + line + "\nChanges in Tables: Since Transaction "+ str(transaction_id) + " is already blocked, e1 is added to list of waiting operations")

                     elif transaction_table[transaction_id]['trans_state'] == 'aborted': # Since this transaction is aborted the items are waiting to be released
                         print("\nExecuting " + line + "\nChanges in Tables: Transaction " + str(transaction_id) +" is already aborted, its current Transaction state is: " + str(transaction_table[transaction_id]['trans_state']))

                     else:
                         commit_transaction = "committed"
                         print("\nExecuting " + line + "\nChanges in Tables: Transaction " + str(transaction_id) + " is committed, and the status is updated to " + str(commit_transaction) + ". The items to be unlocked: " + str(transaction_table[transaction_id]['trans_item']))

                         end_transaction(transaction_id,commit_transaction,line) #  end_transaction is called recursively and the transaction is thereby committed

        elif operation == "r" or operation == "w":
            line_transaction = line.split(" ")
            transaction_id = line_transaction[0][1]
            item_id = line_transaction[1][1]
			
            if transaction_id in transaction_table: # Check if the transaction is available 
                if transaction_table[transaction_id]['trans_state'] == 'blocked': # If the transaction is blocked it has to be updated in waiting transaction and waiting operation's list
                    transaction_table[transaction_id]['list_of_waitingoperations'].append({'operation':operation, 'item_id':item_id}) # The current operatrion is newly added to the list of waiting opeartions in the transaction table  
                    lock_table[item_id]['list_of_transactions_waiting'].append(transaction_id) # This transaction is updated in the list of waiting transactions in the lock table

                elif transaction_table[transaction_id]['trans_state'] == 'aborted':
                    print("\nExecuting " + line + "\nChanges in Tables: transaction " + str(transaction_id) + " is already aborted, and the status is updated to " + str(transaction_table[transaction_id]['trans_state']))             
					
                elif transaction_table[transaction_id]['trans_state'] == 'active': # The read or write operations can only be performed in a transaction only when the transaction is in active state
                    if operation == "r":
                        read_item(line) # Calls read item function
                    if operation == "w":
                        write_item(line)# Calls write item function
    '''for i in transaction_table[transaction_id]['trans_state']:
        if i != "committed":'''
            
            

    print()
    print("Transaction Table:")
    print()
    print(transaction_table)
    print()
    print("Lock Table:")
    print()
    print(lock_table)

    
