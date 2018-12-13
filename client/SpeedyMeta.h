#ifndef CEPH_SPEEDYMETA_CLIENT_H
#define CEPH_SPEEDYMETA_CLIENT_H
#include "include/types.h"

// stl
#include <string>
#include <memory>
#include <set>
#include <map>
#include <fstream>
using std::set;
using std::map;
using std::fstream;

#include "include/unordered_set.h"
#include "include/unordered_map.h"
#include "include/filepath.h"
#include "include/interval_set.h"
#include "include/lru.h"
#include "mds/mdstypes.h"
#include "msg/Dispatcher.h"
#include "msg/Messenger.h"

#include "common/Mutex.h"
#include "common/Timer.h"
#include "common/Finisher.h"
#include "common/compiler_extensions.h"
#include "common/cmdparse.h"
#include "common/CommandTable.h"

#include "osdc/ObjectCacher.h"

#include "InodeRef.h"
#include "UserPerm.h"
#include "include/cephfs/ceph_statx.h"

#include <unistd.h>
#include <sys/types.h>
#include <time.h>
#include <utime.h>
#include <sys/stat.h>
#include <sys/param.h>
#include <fcntl.h>
#include <sys/file.h>
#include <sys/utsname.h>
#include <sys/uio.h>

#include <boost/lexical_cast.hpp>
#include <boost/fusion/include/std_pair.hpp>

#if defined(__FreeBSD__)
#define XATTR_CREATE    0x1
#define XATTR_REPLACE   0x2
#else
#include <sys/xattr.h>
#endif

#if defined(__linux__)
#include <linux/falloc.h>
#endif

#include <sys/statvfs.h>

#include "common/config.h"
#include "common/version.h"

// ceph stuff
#include "messages/MClientSession.h"
#include "messages/MClientReconnect.h"
#include "messages/MClientRequest.h"
#include "messages/MClientRequestForward.h"
#include "messages/MClientReply.h"
#include "messages/MClientCaps.h"
#include "messages/MClientLease.h"
#include "messages/MClientSnap.h"
#include "messages/MCommandReply.h"
#include "messages/MOSDMap.h"
#include "messages/MClientQuota.h"
#include "messages/MClientCapRelease.h"
#include "messages/MMDSMap.h"
#include "messages/MFSMap.h"
#include "messages/MFSMapUser.h"

#include "mon/MonClient.h"

#include "mds/flock.h"
#include "osd/OSDMap.h"
#include "osdc/Filer.h"

#include "common/Cond.h"
#include "common/Mutex.h"
#include "common/perf_counters.h"
#include "common/admin_socket.h"
#include "common/errno.h"
#include "include/str_list.h"

#define dout_subsys ceph_subsys_client

#include "include/lru.h"
#include "include/compat.h"
#include "include/stringify.h"

#include "Client.h"
#include "Inode.h"
#include "Dentry.h"
#include "Delegation.h"
#include "Dir.h"
#include "ClientSnapRealm.h"
#include "Fh.h"
#include "MetaSession.h"
#include "MetaRequest.h"
#include "ObjecterWriteback.h"
#include "posix_acl.h"

#include "include/assert.h"
#include "include/stat.h"

#include "include/cephfs/ceph_statx.h"

#if HAVE_GETGROUPLIST
#include <grp.h>
#include <pwd.h>
#include <unistd.h>
#endif

#define  tout(cct)       if (!cct->_conf->client_trace.empty()) traceout

class FSMap;
class FSMapUser;
class MonClient;

class CephContext;
class MClientReply;
class MClientRequest;
class MClientSession;
class MClientRequest;
class MClientRequestForward;
struct MClientLease;
class MClientCaps;
class MClientCapRelease;

struct DirStat;
struct LeaseStat;
struct InodeStat;

class Filer;
class Objecter;
class WritebackHandler;

class PerfCounters;
class MDSMap;
class Message;

typedef struct dc_v{
  float score;
  unsigned long len;
  long long offset;
}dc_value;

typedef struct feedback_info{
  utime_t time_phd;
  bool accessed;
}feedback;

typedef struct feedback_info_flush{
  uint64_t refer_id;
  uint64_t dc_id;
}feedback_flush;

#define SECOND_FEEDBACK_FLUSH 5
#define SECOND_COUNT_PHD_ACCURACY 10

/* speedymeta_set_dc - add or update data correlation
 * in - target file inode
 * key - dc's name
 * value - dc's value
 * size - size of value
 */
void Client::speedymeta_set_dc(Inode *in, string key, bufferptr bp){
	if(in->xattrs.find(key) == in->xattrs.end())
		in->xattrs[key] = bp;
	else
		in->xattrs.insert(pair<string, bufferptr>(key, bp));
}

/* speedymeta_rm_dc - remove data correlation in inode
 * in - target file inode
 * key - removed key
 */
void Client::speedymeta_rm_dc(Inode *in, string key){
	if(in->xattrs.find(key) == in->xattrs.end())
		in->xattrs.erase(key);
	return;
}

/* speedymeta_list_dc - list all data correlations
 * in - target file inode
 */
void Client::speedymeta_list_dc(Inode *in){
	for(map<string, bufferptr>::iterator it = in->xattrs.begin(); it != in->xattrs.end(); it++){
		dc_value *tmp = (dc_value*)malloc(sizeof(dc_value));
		memcpy(tmp, it->second.c_str(), sizeof(dc_value));
		ldout(cct, 2) << " _SPEEDYMETA " << __func__ << "[" << it->first << " --> (" << tmp->score << "," << tmp->len << "," << tmp->offset << ")]" << dendl;
		free(tmp);
	}
	return;
}

/* speedymeta_mark_dc_dirty - mark dc dirty
 * in - target file inode
 */
void Client::speedymeta_mark_dc_dirty(Inode *in){
	in->xattr_version++;
	mark_caps_dirty(in, CEPH_CAP_XATTR_EXCL);
}

/* speedymeta_cache_xattrs - set data correlation as extend attribute
 * in - target file inode
 * key - xattr's key
 * value - xattr's value where is struct dc_value, includes {score, length, offset}
 * size - size of value
 */
void Client::speedymeta_cache_dc(Inode *in, string key, void *value, uint64_t size){
	bufferptr bp;
	bp = buffer::copy((const char*)value, sizeof(dc_value));

	speedymeta_set_dc(in, key, bp);
	speedymeta_mark_dc_dirty(in);

	return;
}

void Client::speedymeta_cache_rm_dc(Inode *in, string key){
	speedymeta_rm_dc(in, key);
	speedymeta_mark_dc_dirty(in);
	return;
}

/* speedymeta_dc_overlap - judge overlap between data correlation and new content
 * offset - offset of new content
 * len - length of new content
 * dc_offset - offset of data correlation
 * dc_len - length of data correlation
 * return - 1: overlapp
  			0: no overlap
 */
int Client::speedymeta_dc_overlap(uint64_t offset, uint64_t len, uint64_t dc_offset, size_t dc_len){
	if((offset >= dc_offset && offset <= dc_offset + dc_len) || (dc_offset >= offset && dc_offset < offset + len))
		return 1;
	return 0;
}

/* speedymeta_overwrite - check obsolete data correlations exist when overwriting, if obsolete exists, remove locally
 * in - target file indoe
 * offset - offset of new content
 * size - length of new content
 */
void Client::speedymeta_overwrite(Inode *in, uint64_t offset, uint64_t size){
	map<string, bufferptr>::iterator iter;
	dc_value *tmp;
	for(iter = in->xattrs.begin(); iter != in->xattrs.end();){
		tmp = (dc_value*)malloc(sizeof(dc_value));
		memcpy(tmp, iter->second.c_str(), sizeof(dc_value));
		long long dc_offset = tmp->offset;
		size_t dc_len = tmp->len;
		if(dc_offset + dc_len > in->size)
			speedymeta_rm_dc(in, iter++->first);

		if(speedymeta_dc_overlap(offset, size, dc_offset, dc_len)){
			// overlap with new content, hence remove this obsolete data correlation
		    speedymeta_rm_dc(in, iter++->first);
		}
		free(tmp);
	}
	speedymeta_mark_dc_dirty(in);
	return;
}

/* speedymeta_parse_reply - parse received bufferlist, extrace prefethed metadata and link them
 * p - bufferlist iterator of received content
 * request - metadata request
 * reply - MDS reply
 * session - session of client and mds
 * return - # prefetched files
 */
int Client::speedymeta_parse_reply(bufferlist::iterator p, MetaRequest *request, MClientReply *reply,  MetaSession *session, Inode* in){
	ldout(cct, 2) << " _SPEEDYMETA " << "speedymeta_parse_reply" << dendl;
	ConnectionRef con = request->reply->get_connection();
	uint64_t features = con->get_features();
	// inode of prefetched file
	InodeStat ist_phd;
	Inode* in_phd;
	// for num_dn
	int num_dn;
	int count = 0;
	// for prefetched dentry
	InodeStat dirst_phd;
	DirStat dst_phd;
	string dname_phd;
	LeaseStat dlease_phd;
	Inode *diri_phd;
	vector<Inode*> in_singlephd;

	vector<InodeStat> vdirst_phd;
	vector<DirStat> vdst_phd;
	vector<string> vdname_phd;
	vector<LeaseStat> vdlease_phd;

	int tmp; // decode 0 behind each prefetched metadata
	while(p != reply->get_trace_bl().end()){
		::decode(num_dn, p);  // decode depth of dentry
		::decode(tmp, p); // decode 0 behind num_dn
		// ldout(cct, 2) << " _SPEEDYMETA " << "Depth of dentry is " << num_dn << " tmp " << tmp << dendl;

		// decode dentry
		int i = 0;
		while(i < num_dn){
			dirst_phd.decode(p, features); // decode dir inode
			dst_phd.decode(p); // decode dir
			::decode(dname_phd, p); // decode dname
			::decode(dlease_phd, p); // decode lease
			i++;

			if(!inode_map.count(dirst_phd.vino)){
				// not in client cache, so update
				diri_phd = add_update_inode(&dirst_phd, request->sent_stamp, session, request->perms);
				// is it root? no count on root
				if(diri_phd == root){
					update_dir_dist(diri_phd, &dst_phd);
					_ll_get(diri_phd);
					in_singlephd.push_back(diri_phd);
					vdirst_phd.push_back(dirst_phd);
					vdst_phd.push_back(dst_phd);
					vdname_phd.push_back(dname_phd);
					vdlease_phd.push_back(dlease_phd);
					continue;
				}
				// #ifdef _SPEEDYMETA_FEEDBACK
				#ifdef _SPEEDYMETA_MEASUREMENT
				// diri_phd->times_phd = 1;
				// diri_phd->period_times_phd = 1;
				#endif
			}
			else{
				// caches in client cache
				// diri_phd = inode_map[dirst_phd.vino];
				diri_phd = add_update_inode(&dirst_phd, request->sent_stamp, session, request->perms); // updated inode
				// is it root? no count on root
				if(diri_phd == root){
					update_dir_dist(diri_phd, &dst_phd);
					_ll_get(diri_phd);
					in_singlephd.push_back(diri_phd);
					vdirst_phd.push_back(dirst_phd);
					vdst_phd.push_back(dst_phd);
					vdname_phd.push_back(dname_phd);
					vdlease_phd.push_back(dlease_phd);
					continue;
				}
				// #ifdef _SPEEDYMETA_FEEDBACK
				#ifdef _SPEEDYMETA_MEASUREMENT
				// diri_phd->times_phd++;
				// diri_phd->period_times_phd++;
				#endif
			}

			// #ifdef _SPEEDYMETA_FEEDBACK
			#ifdef _SPEEDYMETA_MEASUREMENT
			// set phd flag
			// diri_phd->accessed = false;
			// diri_phd->prefetched = true;
			// total_phd++;
			// period_phd++;
			// ldout(cct, 2) << "_SPEEDYMETA " << " total_phd=" << total_phd << " period_phd=" << period_phd << dendl;
			#endif

			update_dir_dist(diri_phd, &dst_phd);
			_ll_get(diri_phd);
			in_singlephd.push_back(diri_phd);
			vdirst_phd.push_back(dirst_phd);
			vdst_phd.push_back(dst_phd);
			vdname_phd.push_back(dname_phd);
			vdlease_phd.push_back(dlease_phd);
		}

		// decode file inode
		ist_phd.decode(p, features);
		count++;
		// if(!inode_map.count(ist_phd.vino)){
		// 	// not in client cache
		// 	in_phd = add_update_inode(&ist_phd, request->sent_stamp, session, request->perms);
		// 	in_phd->times_phd = 1;
		// 	in_phd->period_times_phd = 1;
		// }
		// else{
		// 	in_phd = add_update_inode(&ist_phd, request->sent_stamp, session, request->perms);
		// 	in_phd->times_phd++;
		// 	in_phd->period_times_phd++;
		// }
		in_phd = add_update_inode(&ist_phd, request->sent_stamp, session, request->perms);
		_ll_get(in_phd);
		in_singlephd.push_back(in_phd);
		// ldcout(cct, 2) << " _SPEEDYMETA " << "Prefetching inode " << in_phd << dendl;
		// #ifdef _SPEEDYMETA_FEEDBACK
		#ifdef _SPEEDYMETA_MEASUREMENT
		in_phd->accessed = false;
		in_phd->prefetched = true;
		in_phd->times_phd++;
		in_phd->period_times_phd++;
		total_phd++;
        period_phd++;
        #endif

		// link dentry and inode insides dir
		unsigned int j = 0;
		while(j < in_singlephd.size() - 1){
			int no_dir = j;
			int no_in = j+1; 
			Dir *dir_phd = in_singlephd[no_dir]->open_dir();
			insert_dentry_inode(dir_phd, vdname_phd[no_dir], &vdlease_phd[no_dir], in_singlephd[no_in], request->sent_stamp, session, NULL);
			j++;
		}

		#ifdef _SPEEDYMETA_FEEDBACK
		std::unordered_map<uint64_t, std::unordered_map<uint64_t, struct feedback_info*>>::iterator iter_key;
        if(!table_feedback.count(in_phd->ino.val)){
          // in_phd is not in table_feedback, so insert it
          feedback* new_feedback = (feedback*)malloc(sizeof(feedback_info));
          new_feedback->time_phd = ceph_clock_now();
          new_feedback->accessed = false;
          std::unordered_map<uint64_t, struct feedback_info*> hmap_tmp;
          hmap_tmp[in->ino.val]=new_feedback;
          table_feedback[in_phd->ino.val] = hmap_tmp;
        } 
        else{
          // in_phd has been existed, hence check whether in is in in_phd entry
          if(!table_feedback[in_phd->ino.val].count(in->ino.val)){
            // in is not in this in_phd entry, insert in into in_phd entry
            feedback* new_sub_feedback = (feedback*)malloc(sizeof(feedback_info));
            new_sub_feedback->time_phd = ceph_clock_now();
            new_sub_feedback->accessed = false;
            table_feedback[in_phd->ino.val].insert(std::pair<uint64_t, struct feedback_info*>(in->ino.val, new_sub_feedback));
          }
        }
		#endif

		// clean up arrays for dentry and inode
		in_singlephd.clear();
		vdirst_phd.clear();
		vdst_phd.clear();
		vdname_phd.clear();
		vdlease_phd.clear();
	}
	return count;
}

#ifdef _SPEEDYMETA_MEASUREMENT
/* speedymeta_count_phd_accuracy - count prefetching accuracy periodly
 * this is a thread
 */
void *Client::speedymeta_count_phd_accuracy(void *args){
	Client *client = reinterpret_cast<Client*>(args);
	ldout(client->Dispatcher::cct, 0) << __func__ << dendl;

	float accuracy_period_phd = 0;
	float accuracy_total_phd = 0;

	while(1){
		sleep(SECOND_COUNT_PHD_ACCURACY);

		// count phd accuracy
		// #ifdef _SPEEDYMETA_FEEDBACK
		if(client->total_phd != 0 && client->period_phd != 0){
			accuracy_total_phd = client->total_phd_access/client->total_phd;
			accuracy_period_phd = client->period_phd_access/client->period_phd;
			// ldout(client->Dispatcher::cct, 0) << " _SPEEDYMETA " << __func__ << " [Accuracy-total-phd] = " << client->total_phd_access << "/" << client->total_phd << " " << accuracy_total_phd << dendl;
			// ldout(client->Dispatcher::cct, 0) << " _SPEEDYMETA " << __func__ << " [Accuracy-period-phd] = " << client->period_phd_access << "/" << client->period_phd << " " << accuracy_period_phd << dendl;
		}
		// clean up parameters
		client->period_phd = 0;
		client->period_phd_access = 0;
		client->access_sequence.clear();
		for(ceph::unordered_map<vinodeno_t, Inode*>::iterator it = client->inode_map.begin(); it != client->inode_map.end(); it++)
			it->second->period_times_phd = 0;
		// #endif
		ldout(client->Dispatcher::cct, 0) << " _SPEEDYMETA " << __func__ << " [OPS] " << client->ops << dendl;
		ldout(client->Dispatcher::cct, 0) << " _SPEEDYMETA " << __func__ << " [OP-ACCESS] " << client->op_access << dendl;
		// ldout(client->Dispatcher::cct, 0) << " _SPEEDYMETA " << __func__ << " [OP-HIT] " << client->op_hit << dendl;
		ldout(client->Dispatcher::cct, 0) << " _SMETA [CACHE] " << "op_access " << client->op_access << " dir_hit " << client->op_hit_dir << " dir_miss " << client->op_miss_dir << " file_hit " << client->op_hit_file << " fiel_miss " << client->op_miss_file << " total_hit " << client->op_hit << dendl;
		// ldout(client->Dispatcher::cct, 0) << " _SPEEDTMETA " << __func__ << " [CACHE] " << " Total: hit/access " << client->op_hit << "/" << client->op_access << "=" << ((double)client->op_hit)/((double)client->op_access) << dendl;
		// ldout(client->Dispatcher::cct, 0) << " _SPEEDTMETA " << __func__ << " [CACHE] " << " Dir: hit " << client->op_hit_dir << " miss " << client->op_miss_dir << " Total:hit/access "  << client->op_hit_dir << "/" << client->op_access << "=" << ((double)client->op_hit_dir)/((double)client->op_access) << dendl;
		// ldout(client->Dispatcher::cct, 0) << " _SPEEDTMETA " << __func__ << " [CACHE] " << " File: hit " << client->op_hit_file << " miss " << client->op_miss_file << " Total:hit/access "  << client->op_hit_file << "/" << client->op_access << "=" << ((double)client->op_hit_file)/((double)client->op_access) << dendl;
		ldout(client->Dispatcher::cct, 0) << " _SPEEDYMETA " << __func__ << " [OP-METAIO] " << client->op_metaio << dendl;
	}

	return NULL;
}
#endif

#ifdef _SPEEDYMETA_FEEDBACK

/* speedymeta_show_table_feedback - print feedback table
 */
void Client::speedymeta_show_table_feedback(){
  ldout(cct, 2) << __func__  << dendl;
  std::unordered_map<uint64_t, std::unordered_map<uint64_t, struct feedback_info*>>::iterator iter = table_feedback.begin();
  for(; iter != table_feedback.end(); iter++){
    std::unordered_map<uint64_t, struct feedback_info*>::iterator it_m = iter->second.begin();
    while(it_m != iter->second.end()){
      // ldout(cct, 2) << __func__ << " " << iter->first << " | " << it_m->first << " | " << it_m->second->time_phd << " | " << it_m->second->score_update << dendl;
      it_m++;
    }
  }
  return;
}

/* speedymeta_show_access_sequence - print access sequence
 */
void Client::speedymeta_show_access_sequence(){
  ldout(cct, 2) << __func__  << dendl;
  std::vector<uint64_t>::iterator iter= access_sequence.begin();
  for(; iter != access_sequence.end(); iter++)
    ldout(cct, 2) << __func__ << " " << *iter << dendl;
  return;
}

/* speedymeta_update_feedback - update feedback table when prefetched inode is accessed
 * ino - inode number
 */
void Client::speedymeta_update_feedback(uint64_t ino){
	if(access_sequence.size() <= 1)
		return;

	std::vector<uint64_t>::reverse_iterator riter = access_sequence.rbegin();
	std::unordered_map<uint64_t, struct feedback_info*>::iterator it;
	riter++;
	for(; riter != access_sequence.rend() && *riter != ino; riter++){
		if((it = table_feedback[ino].find(*riter)) != table_feedback[ino].end()){
			it->second->accessed = true;
		}
	}
	speedymeta_show_table_feedback();
	return;
}

/* speedymeta_access - update access sequence for each access and update feedback table if it is prefetched
 * ino - inode number
 * prefetched - is prefetched?
 */
void Client::speedymeta_access(uint64_t ino, bool prefetched){
	access_sequence.push_back(ino);
	speedymeta_show_access_sequence();
	if(prefetched){
		speedymeta_update_feedback(ino);
	}
	return;
}

/* speedymeta_send_feedback - send feedback result to mds
 * request - metadata request with feedback result
 */
void Client::speedymeta_send_feedback(MetaRequest* request){
	request->set_tid(++last_tid);
	request->op_stamp = ceph_clock_now();
	request->resend_mds = rand() % mdsmap->get_num_in_mds();
	// get a random mds
	while(1){
	mds_rank_t mds;
	std::set<mds_rank_t> up;
	mdsmap->get_up_mds_set(up);
	if(up.empty())
	  mds = -1;
	std::set<mds_rank_t>::const_iterator p = up.begin();
	for(int n = rand() % up.size(); n; n--)
	  ++p;
	mds = *p;
	ldout(cct, 2) << __func__ << " mds = " << mds << dendl;
	if(mds < MDS_RANK_NONE || !mdsmap->is_active_or_stopping(mds)){
	  ldout(cct, 2) << __func__ << " target mds." << mds << " not active, waiting for new mdsmap" << dendl;
	  wait_on_list(waiting_for_mdsmap);
	  continue;
	}

	MetaSession *session = NULL;
	if(!have_open_session(mds)){
	  if(!mdsmap->is_active_or_stopping(mds)){
	    ldout(cct, 2) << __func__ << " no address for mds." << mds << ", waiting for new mdsmap" << dendl;
	    wait_on_list(waiting_for_mdsmap);
	  }
	  if(!mdsmap->is_active_or_stopping(mds)){
	    ldout(cct, 2) << __func__ << " hmm, still have no address for mds." << mds << ", trying a random mds" << dendl;
	    p = up.begin();
	    for(int n = rand() % up.size(); n; n--)
	      ++p;
	    request->resend_mds = *p;
	    continue;
	  }
	  session = _get_or_open_mds_session(mds);
	  if(session->state == MetaSession::STATE_OPENING){
	    ldout(cct, 2) << __func__ << " waiting for session to mds." << mds << " to open" << dendl;
	    wait_on_context_list(session->waiting_for_open);
	    if(rejected_by_mds.count(mds)){
	      ldout(cct, 2) << __func__ << " rejected by mds." << mds << dendl;
	      request->abort(-EPERM);
	      break;
	    }
	    continue;
	  }

	  if(!have_open_session(mds))
	    continue;
	}
	else
	  session = mds_sessions[mds];

	send_request(request, session);
	ldout(cct, 2) << __func__ << " send request to mds." << mds << dendl;
	break;
	}
	return;
}

/* adaptive_feedback - send feedback to mds periodly
 * this is a thread
 */
void *Client::speedymeta_adaptive_feedback(void *args){
	Client *client = reinterpret_cast<Client*>(args);
	ldout(client->Dispatcher::cct, 1) << __func__ << dendl;
	bufferlist bl;
	int flush = 1;

	float accuracy_total_phd = 0;
	float accuracy_period_phd = 0;
	uint64_t traffic_feedback = 0;
	while(1){
		bl.clear();
		sleep(SECOND_FEEDBACK_FLUSH);
		traffic_feedback = 0;

		// traverse feedback table and batch out-of-date entry to build feedbak into bl
		std::unordered_map<uint64_t, std::unordered_map<uint64_t, struct feedback_info*>>::iterator iter = client->table_feedback.begin();
		for(; iter != client->table_feedback.end(); iter++){
			std::unordered_map<uint64_t, struct feedback_info*>::iterator it = iter->second.begin();
			for(; it != iter->second.end(); ){
				// extract out-of-date entry and clean up
				if(ceph_clock_now() - it->second->time_phd >= SECOND_FEEDBACK_FLUSH){
					if(it->second->accessed == false){
						// not accessed, hence batch it into bl and flush
						feedback_flush *item = new feedback_flush();
						item->refer_id = it->first;
						item->dc_id = iter->first;
						ldout(client->Dispatcher::cct, 2) << " _SPEEDYMETA " << __func__ << " " << item->refer_id << "-->" << item->dc_id << dendl;

						bl.append(reinterpret_cast<const char*>(item), sizeof(feedback_flush));
						traffic_feedback += sizeof(feedback_flush);
						free(it->second);
						delete item;
					}
					it = iter->second.erase(it);
				}
				else
					it++;
			}
		}

		// send feedback bufferlist to mds
		if(traffic_feedback != 0){
			MetaRequest *request = new MetaRequest(CEPH_MDS_OP_FEEDBACK);
			request->op_stamp = ceph_clock_now();
			request->set_data(bl);
			client->speedymeta_send_feedback(request);
		}
		if(flush % 2 == 0){
			// if(client->total_phd != 0 && client->period_phd != 0){
			// 	accuracy_total_phd = client->total_phd_access/client->total_phd;
			// 	accuracy_period_phd = client->period_phd_access/client->period_phd;
			// 	ldout(client->Dispatcher::cct, 1) << " _SPEEDYMETA " << __func__ << " [Accuracy-total-phd] = " << client->total_phd_access << "/" << client->total_phd << " " << accuracy_total_phd << dendl;
			// 	ldout(client->Dispatcher::cct, 1) << " _SPEEDYMETA " << __func__ << " [Accuracy-period-phd] = " << client->period_phd_access << "/" << client->period_phd << " " << accuracy_period_phd << dendl;
			// }
			// // clean up parameters
			// client->period_phd = 0;
			// client->period_phd_access = 0;
			// client->access_sequence.clear();
			// for(ceph::unordered_map<vinodeno_t, Inode*>::iterator it = client->inode_map.begin(); it != client->inode_map.end(); it++)
			// 	it->second->period_times_phd = 0;
		}

		flush++;
	}
	client->table_feedback.clear();
	return NULL;
}

#endif

#endif
