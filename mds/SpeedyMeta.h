#ifndef CEPH_SPEEDYMETA_MDS_SERVER_H
#define CEPH_SPEEDYMETA_MDS_SERVER_H

#include <boost/lexical_cast.hpp>
#include "include/assert.h"  // lexical_cast includes system assert.h

#include <boost/config/warning_disable.hpp>
#include <boost/fusion/include/std_pair.hpp>

#include "MDSRank.h"
#include "Server.h"
#include "Locker.h"
#include "MDCache.h"
#include "MDLog.h"
#include "Migrator.h"
#include "MDBalancer.h"
#include "InoTable.h"
#include "SnapClient.h"
#include "Mutation.h"

#include "msg/Messenger.h"

#include "osdc/Objecter.h"

#include "messages/MClientSession.h"
#include "messages/MClientRequest.h"
#include "messages/MClientReply.h"
#include "messages/MClientReconnect.h"
#include "messages/MClientCaps.h"
#include "messages/MClientSnap.h"

#include "messages/MMDSSlaveRequest.h"

#include "messages/MLock.h"

#include "events/EUpdate.h"
#include "events/ESlaveUpdate.h"
#include "events/ESession.h"
#include "events/EOpen.h"
#include "events/ECommitted.h"

#include "include/filepath.h"
#include "common/errno.h"
#include "common/Timer.h"
#include "common/perf_counters.h"
#include "include/compat.h"
#include "osd/OSDMap.h"

#include <errno.h>

#include <list>
#include <iostream>
#include <boost/utility/string_view.hpp>
using namespace std;

#include "common/config.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds." << mds->get_nodeid() << ".server "


float SCORE_THRESHOLD = 0.5;
float SCORE_INC = 0.05;
float SCORE_DEC = -0.1;

typedef struct dc_v{
  float score;
  unsigned long len;
  long long offset;
}dc_value;

/* speedymeta_get_dcscore - extract score of a corresponding data correlation and return
 * cin - target file inode
 * path - filepath of related file
 * return - score of path
 */
float Server::speedymeta_get_dcscore(CInode *cin, string path){
	auto dclist = cin->get_projected_xattrs();
	auto px = dclist->begin();
	for(; px != dclist->end(); px++){
		if(std::string(boost::string_view(px->first)).compare(path) == 0)
			break;
	}

	// dout(2) << " _SPEEDYMETA " << " key=" << px->first << " length=" << px->second.length() << dendl;
	char* tmp = (char*)malloc(px->second.length());
	memcpy(tmp, px->second.c_str(), px->second.length());
	dout(2) << " _SPEEDYMETA " << tmp << dendl;
	dc_value *dv = (dc_value*)malloc(sizeof(dc_value));
	sscanf(tmp,"%f,%ld,%lld", &(dv->score),&(dv->len),&(dv->offset));

	float score;
	score = dv->score;
	dout(2) << " _SPEEDYMETA " << " PATH:" << path << " score:" << score << " len:" << dv->len << " offset:" << dv->offset << dendl;
	free(tmp);
	free(dv);
	return score;
}

/* speedymeta_add_dcscore - add score of a corresponding data correlation and return
 * cin - target file inode
 * path - filepath of related file
 * d_score - score changes, if d_score < 0 , decrease score;
 * return - updated score
 */
float Server::speedymeta_add_dcscore(CInode *cin, string path, float d_score){
	auto dclist = cin->get_projected_xattrs();
	auto px = dclist->begin();
	for(; px != dclist->end(); px++){
		if(std::string(boost::string_view(px->first)).compare(path) == 0)
			break;
	}

	char* tmp = (char*)malloc(px->second.length());
	memcpy(tmp, px->second.c_str(), px->second.length());
	dout(2) << " _SPEEDYMETA " << tmp << dendl;
	// dc_value *dv = reinterpret_cast<dc_value*>(tmp);
	dc_value *dv = (dc_value*)malloc(sizeof(dc_value));
	sscanf(tmp,"%f,%ld,%lld", &(dv->score),&(dv->len),&(dv->offset));
	
	dv->score += d_score;
	float updated_score = dv->score;
	char *value = (char*)malloc(sizeof(dc_value)+2);
	sprintf(value, "%f,%ld,%lld", dv->score, dv->len, dv->offset);
	px->second = buffer::copy((const char*)value, sizeof(dc_value)+2); // not sure
	free(tmp);
	free(dv);
	free(value);
	dout(2) << " _SPEEDYMETA " << " updated score = " << updated_score << dendl;
	return updated_score;
}

/* speedymeta_prefetch - prefetch related metadata
 * mdr - metadata request
 * ref - target file inode
 */
int Server::speedymeta_prefetch(MDRequestRef& mdr, CInode *ref){
	vector<CInode*> vin_phd; // array of prefetched inodes
	vector<CDentry*> vdn_phd; // array of dentry of prefetched file
	vector<vector<CDentry*>> vvdn_phd; // batched array of dentry of prefetched files

	set<SimpleLock*> phd_rdlocks, phd_wrlocks, phd_xlocks; // locks for prefetched files
	// std::string s_ref;
	// ref->make_path_string(s_ref);
	// dout(2) << " _SPEEDYMETA " << " target filepath:" << s_ref << dendl;

	CInode *in_phd;
	auto dclist = ref->get_projected_xattrs();
	auto px = dclist->begin();
	// map<string, bufferptr>::iterator pxat = px->begin();
	dout(2) << " _SPEEDYMETA " << " dc's size = " << dclist->size() << dendl;

	int mask = mdr->client_request->head.args.getattr.mask;

	#ifdef _SPEEDYMETA_DC
	for(; px != dclist->end(); px++){
		// traverse data correlation and prefetch closer data correlation
		// if(strncmp(px->first.c_str(), "user.dc.", 8) != 0) // this entry is not data correlation, hence skip
		// 	continue;
		// std::string path_phd = std::string(boost::string_view(px->first)).substr(8);
		std::string path_phd = std::string(boost::string_view(mempool::mds_co::string(px->first)));
		// dout(2) << " _SPEEDYMETA " << " target filepath:" << s_ref << dendl;
		dout(2) << " _SPEEDYMETA_DC " << " traversing xattr " << path_phd << dendl;
		if(path_phd.compare(0,1,"/") != 0)
			continue;
		
		if(speedymeta_get_dcscore(ref, path_phd) >= SCORE_THRESHOLD){
			// score is higher than threshold, prefetch it now!
			dout(2) << " _SPEEDYMETA_DC " << " prefetching " << path_phd << "..." << dendl;
				
			// clean up tmp structure
			vdn_phd.clear();
			in_phd = NULL;

			filepath fp_phdref(path_phd);
			// fp_phdref.push_dentry(path_phd);
			std::string tmp_s = fp_phdref.get_path();
			dout(2) << " _SPEEDYMETA_DC " << "1" << dendl;
			unsigned tmp_dep = fp_phdref.depth();
			dout(2) << " _SPEEDYMETA_DC " << "2" << dendl;
			int tmp_len = fp_phdref.length();
			dout(2) << " _SPEEDYMETA_DC " << fp_phdref.c_str() << " path:" << tmp_s << " depth:" << tmp_dep << " length " << tmp_len << dendl;
			
			int r = mdcache->path_traverse(mdr, NULL, NULL, fp_phdref, &vdn_phd, &in_phd, MDS_TRAVERSE_FORWARD);
			dout(2) << " _SPEEDYMETA_DC " << " res(path_traverse) = " << r << dendl;
			if(r > 0) return r;  // delay
			if(in_phd == NULL){
				// file is no longer exist and delete this dc now
				dout(2) << " _SPEEDYMETA_DC " << " inode is NULL" << dendl;
				#ifdef _SPEEDYMETA_DELETE
				dclist->erase(px++);
				ref->project_inode(true).inode.xattr_version++;
				ref->mark_dirty(ref->pre_dirty() + 1, mdlog->get_current_segment());
				dout(2) << " _SPEEDYMETA_DC " << " file " << path_phd << " no longer exists and delete this data correlation now." << dendl;
				#endif
				continue;
			}
			if(in_phd->ino() == ref->ino())
				continue;

			dout(2) << " _SPEEDYMETA_DC " << *in_phd << dendl;

			// file exist and prefetch now
			filepath fp_phd;
			fp_phd.set_path(path_phd.c_str());
			if(vdn_phd.size() == fp_phd.depth()){
				dout(2) << " _SPEEDYMETA_DC " << " # dentry = depth" << dendl;

				// insert to prefetch list
				vin_phd.push_back(in_phd);
				vvdn_phd.push_back(vdn_phd);

			}
		}
	}
	#endif

	#ifdef _SPEEDYMETA_AC
	std::string s_ref;
	ref->make_path_string(s_ref);
	dout(2) << " _SPEEDYMETA_AC " << " target filepath:" << s_ref << dendl;

	// ref has no access correlations, return without prefetching
	if(list_ac.find(s_ref) != list_ac.end()){

		// CInode *in_phd;

		// int mask = mdr->client_request->head.args.getattr.mask;

		// list access correlations
		std::vector<string>::iterator it = list_ac[s_ref].begin();
		for(; it != list_ac[s_ref].end(); it++){
			dout(2) << __func__ << " " << s_ref << " --> " << *it << dendl;

			std::string path_phd = *it;

			dout(2) << " _SPEEDYMETA_AC " << " traversing xattr " << path_phd << dendl;
			if(path_phd.compare(0,1,"/") != 0)
				continue;
			
			// score is higher than threshold, prefetch it now!
			dout(2) << " _SPEEDYMETA_AC " << " prefetching " << path_phd << "..." << dendl;
				
			// clean up tmp structure
			vdn_phd.clear();
			in_phd = NULL;

			filepath fp_phdref(path_phd);
			// fp_phdref.push_dentry(path_phd);
			std::string tmp_s = fp_phdref.get_path();
			dout(2) << " _SPEEDYMETA_AC " << "1" << dendl;
			unsigned tmp_dep = fp_phdref.depth();
			dout(2) << " _SPEEDYMETA_AC " << "2" << dendl;
			int tmp_len = fp_phdref.length();
			dout(2) << " _SPEEDYMETA_AC " << fp_phdref.c_str() << " path:" << tmp_s << " depth:" << tmp_dep << " length " << tmp_len << dendl;
			
			int r = mdcache->path_traverse(mdr, NULL, NULL, fp_phdref, &vdn_phd, &in_phd, MDS_TRAVERSE_FORWARD);
			dout(2) << " _SPEEDYMETA_AC " << " res(path_traverse) = " << r << dendl;
			if(r > 0) return r;  // delay
			if(in_phd == NULL){
				// file is no longer exist and delete this dc now
				dout(2) << " _SPEEDYMETA_AC " << " inode is NULL" << dendl;
				#ifdef _SPEEDYMETA_DELETE
				dclist->erase(px++);
				ref->project_inode(true).inode.xattr_version++;
				ref->mark_dirty(ref->pre_dirty() + 1, mdlog->get_current_segment());
				dout(2) << " _SPEEDYMETA_AC " << " file " << path_phd << " no longer exists and delete this data correlation now." << dendl;
				#endif
				continue;
			}
			if(in_phd->ino() == ref->ino())
				continue;

			dout(2) << " _SPEEDYMETA_AC " << *in_phd << dendl;

			// file exist and prefetch now
			filepath fp_phd;
			fp_phd.set_path(path_phd.c_str());
			if(vdn_phd.size() == fp_phd.depth()){
				dout(2) << " _SPEEDYMETA_AC " << " # dentry = depth" << dendl;

				// insert to prefetch list
				vin_phd.push_back(in_phd);
				vvdn_phd.push_back(vdn_phd);

			}
		}
	}
	#endif

	// lock and check access
	vector<CInode*>::iterator i_it = vin_phd.begin();
	vector<vector<CDentry*>>::iterator dn_it = vvdn_phd.begin();
	while(i_it != vin_phd.end() && dn_it != vvdn_phd.end()){
		in_phd = *i_it;
		vdn_phd = *dn_it;
		// clean up locks
		phd_rdlocks.clear();
		phd_wrlocks.clear();
		phd_xlocks.clear();

		// dentry loss?
		for(int i = 0; i < (int)vdn_phd.size(); i++)
			phd_rdlocks.insert(&vdn_phd[i]->lock);
		mds->locker->include_snap_rdlocks(phd_rdlocks, in_phd);
		mdr->pin(in_phd);
		
		int phd_issued = 0;
		Capability *phd_cap = in_phd->get_client_cap(mdr->get_client());
		if(phd_cap && (mdr->snapid == CEPH_NOSNAP || mdr->snapid <= phd_cap->client_follows))
			phd_issued = phd_cap->issued();
		if ((mask & CEPH_CAP_LINK_SHARED) && !(phd_issued & CEPH_CAP_LINK_EXCL)) phd_rdlocks.insert(&in_phd->linklock);
		if ((mask & CEPH_CAP_AUTH_SHARED) && !(phd_issued & CEPH_CAP_AUTH_EXCL)) phd_rdlocks.insert(&in_phd->authlock);
		if ((mask & CEPH_CAP_XATTR_SHARED) && !(phd_issued & CEPH_CAP_XATTR_EXCL)) phd_rdlocks.insert(&in_phd->xattrlock);
		if ((mask & CEPH_CAP_FILE_SHARED) && !(phd_issued & CEPH_CAP_FILE_EXCL)) {
		    // Don't wait on unstable filelock if client is allowed to read file size.
		    // This can reduce the response time of getattr in the case that multiple
		    // clients do stat(2) and there are writers.
		    // The downside of this optimization is that mds may not issue Fs caps along
		    // with getattr reply. Client may need to send more getattr requests.
		    if (mdr->rdlocks.count(&in_phd->filelock)) {
		      phd_rdlocks.insert(&in_phd->filelock);
		    } else if (in_phd->filelock.is_stable() ||
			       in_phd->filelock.get_num_wrlocks() > 0 ||
			       !in_phd->filelock.can_read(mdr->get_client())) {
		      phd_rdlocks.insert(&in_phd->filelock);
		      mdr->done_locking = false;
		    }
		}
		
		// locking preteched metadata is possible, if failed, no prefetching.
		bool locking = mdr->done_locking;
		if(locking)
			mdr->done_locking = false;
		if(!mds->locker->acquire_locks(mdr, phd_rdlocks, phd_wrlocks, phd_xlocks)){
			dout(2) << " _SPEEDYMETA " << " locking failed." << dendl;
			mdr->done_locking = locking;
			i_it = vin_phd.erase(i_it);
			dn_it = vvdn_phd.erase(dn_it);
			continue;
			// return -1;
		}
		
		if(!check_access(mdr, in_phd, MAY_READ)){
			i_it = vin_phd.erase(i_it);
			dn_it = vvdn_phd.erase(dn_it);
			continue;
		}

		#ifdef _SPEEDYMETA_FEEDBACK
		speedymeta_add_dcscore(ref, path_phd, SCORE_INC);
		#endif
		
		i_it++;
		dn_it++;
	}


	// attach prefetched metadata into metadata request
	mdr->tracei_phd = vin_phd;
	mdr->tracedn_phd = vvdn_phd;

	return 0;
}

/* speedymeta_batch_reply - batch prefetched metadata into bufferlist
 * mdr - metadata request
 * bl - bufferlist replied
 * session - session between from client to mds
 * snapid - snapid
 * return - # of batched files
 */
int Server::speedymeta_batch_reply(MDRequestRef& mdr, bufferlist *bl, Session *session, snapid_t snapid){
	vector<CInode*> tracei_phd = mdr->tracei_phd;
	vector<vector<CDentry*>> tracedn_phd = mdr->tracedn_phd;

	client_t client = session->get_client();
	utime_t now = ceph_clock_now();

	int num_phd = 0;
	int i = 0;
	mds_rank_t whoami = mds->get_nodeid();

	dout(2) << " _SPEEDYMETA " << "Batching prefetched metadata..." << dendl;
	for(vector<vector<CDentry*>>::iterator iter = tracedn_phd.begin(); iter != tracedn_phd.end(); iter++, i++){
		dout(2) << " _SPEEDYMETA " << "# dentry is " << (*iter).size() << dendl;
		::encode(tracedn_phd[i].size(), *bl); // encode depth of entry
		
		// batch each dentry  into bl
		for(vector<CDentry*>::iterator it = (*iter).begin(); it != (*iter).end(); it++){
			CDir *dir_phd = (*it)->get_dir();
			CInode *diri_phd = dir_phd->get_inode();
			diri_phd->encode_inodestat(*bl, session, NULL, snapid); // encode dir inode
			#ifdef MDS_VERIFY_FRAGSTAT
			if(dir_phd->is_complete())
				dir_phd->verify_fragstat();
			#endif
			dir_phd->encode_dirstat(*bl, whoami); // encode directory
			::encode((*it)->get_name(), *bl); // encode dir name
			if(snapid == CEPH_NOSNAP)
				mds->locker->issue_client_lease((*it), client, *bl, now, session);
			else
				encode_null_lease(*bl);
		}

		// encode file inode
		tracei_phd[i]->encode_inodestat(*bl, session, NULL, snapid, 0, mdr->getattr_caps);

		// increase number of batched file
		num_phd++;
	}

	// clean up mdr->tracei_phd and mdr->tracedn_phd
	mdr->tracei_phd.clear();
	mdr->tracedn_phd.clear();

	dout(2) << " _SPEEDYMETA " << "# files prefetched is " << num_phd << dendl;
	return num_phd;
}

#ifdef _SPEEDYMETA_FEEDBACK
/* speedymeta_handle_client_feedback - handle client feedback result
 * mdr - received feedback result
 */
void Server::speedymeta_handle_client_feedback(MDRequestRef& mdr){
	dout(2) << " _SPEEDYMETA " << __func__ << dendl;
	MClientRequest *req = mdr->client_request;
	bufferlist::iterator p = req->get_data().begin();
	uint64_t flush_refer_id, flush_dc_id;
	// float flush_score_update;
	// int zero;
	// void *tem;

	while(p != req->get_data().end()){
		::decode(flush_refer_id, p); // decode refered file id
		::decode(flush_dc_id, p); // decode dc file id
		dout(2) << " _SPEEDYMETA " << __func__ << flush_refer_id << " --> " << flush_dc_id << dendl;

		CInode *target = mdcache->get_inode(flush_refer_id);
		CInode *dc = mdcache->get_inode(flush_dc_id);
		filepath fp_dc;
		dc->make_path(fp_dc);
		float updated_score = speedymeta_add_dcscore(target, "/" + fp_dc.get_path(), SCORE_DEC);
		dout(2) << " _SPEEDYMETA_FEEDBACK " << "Updated score = " << updated_score << dendl;
	}
	mdcache->request_finish(mdr);
	return;
}
#endif

#ifdef _SPEEDYMETA_AC
// access correlation initialization
void Server::_init_ac(){
	dout(0) << __func__ << " init done." << dendl;
	char cwd[PATH_MAX];
   	if(getcwd(cwd, sizeof(cwd)) != NULL) {
   		dout(0) << __func__ << " PWD:" << cwd << dendl;
   	}
   	else
   		return;

	FILE *fp_ac = fopen("/list_ac", "r");
	if(fp_ac == NULL){
		dout(0) << __func__ << " open ac file error." << dendl;
		dout(0) << __func__ << " Erroring - " << strerror(errno) << dendl; 
		return;
	}

	// read ac list and pin to memory
	char target[10240], related[10240];
	while(1){
		fscanf(fp_ac, "%s\t%s", target, related);
		// dout(0) << __func__ << " " << target << " -> " << related << dendl;
		if(feof(fp_ac))
			break;

		list_ac[target].push_back(related);
	}

	// show all ac
	std::unordered_map<string, std::vector<string>>::iterator it = list_ac.begin();
	for(; it != list_ac.end(); it++){
		std::vector<string>::iterator itv = it->second.begin();
		for(; itv != it->second.end(); itv++)
			dout(0) << __func__ << " " << it->first << " -> " << *itv << dendl;
	}

	fclose(fp_ac);

	return;
}

// prefetching based on access correlation via C-Miner
int Server::speedymeta_prefetch_ac(MDRequestRef& mdr, CInode *ref){
	if(ref == NULL)
		return 0;

	vector<CInode*> vin_phd; // array of prefetched inodes
	vector<CDentry*> vdn_phd; // array of dentry of prefetched file
	vector<vector<CDentry*>> vvdn_phd; // batched array of dentry of prefetched files

	set<SimpleLock*> phd_rdlocks, phd_wrlocks, phd_xlocks; // locks for prefetched files

	std::string s_ref;
	ref->make_path_string(s_ref);
	dout(2) << " _SPEEDYMETA " << " target filepath:" << s_ref << dendl;

	// ref has no access correlations, return without prefetching
	if(list_ac.find(s_ref) == list_ac.end())
		return 0;

	CInode *in_phd;

	int mask = mdr->client_request->head.args.getattr.mask;

	// list access correlations
	std::vector<string>::iterator it = list_ac[s_ref].begin();
	for(; it != list_ac[s_ref].end(); it++){
		dout(2) << __func__ << " " << s_ref << " --> " << *it << dendl;

		std::string path_phd = *it;

		dout(2) << " _SPEEDYMETA " << " traversing xattr " << path_phd << dendl;
		if(path_phd.compare(0,1,"/") != 0)
			continue;
		
		// score is higher than threshold, prefetch it now!
		dout(2) << " _SPEEDYMETA " << " prefetching " << path_phd << "..." << dendl;
			
		// clean up tmp structure
		vdn_phd.clear();
		in_phd = NULL;

		filepath fp_phdref(path_phd);
		// fp_phdref.push_dentry(path_phd);
		std::string tmp_s = fp_phdref.get_path();
		dout(2) << " _SPEEDYMETA " << "1" << dendl;
		unsigned tmp_dep = fp_phdref.depth();
		dout(2) << " _SPEEDYMETA " << "2" << dendl;
		int tmp_len = fp_phdref.length();
		dout(2) << " _SPEEDYMETA " << fp_phdref.c_str() << " path:" << tmp_s << " depth:" << tmp_dep << " length " << tmp_len << dendl;
		
		int r = mdcache->path_traverse(mdr, NULL, NULL, fp_phdref, &vdn_phd, &in_phd, MDS_TRAVERSE_FORWARD);
		dout(2) << " _SPEEDYMETA " << " res(path_traverse) = " << r << dendl;
		if(r > 0) return r;  // delay
		if(in_phd == NULL){
			// file is no longer exist and delete this dc now
			dout(2) << " _SPEEDYMETA " << " inode is NULL" << dendl;
			#ifdef _SPEEDYMETA_DELETE
			dclist->erase(px++);
			ref->project_inode(true).inode.xattr_version++;
			ref->mark_dirty(ref->pre_dirty() + 1, mdlog->get_current_segment());
			dout(2) << " _SPEEDYMETA " << " file " << path_phd << " no longer exists and delete this data correlation now." << dendl;
			#endif
			continue;
		}
		if(in_phd->ino() == ref->ino())
			continue;

		dout(2) << " _SPEEDYMETA " << *in_phd << dendl;

		// file exist and prefetch now
		filepath fp_phd;
		fp_phd.set_path(path_phd.c_str());
		if(vdn_phd.size() == fp_phd.depth()){
			dout(2) << " _SPEEDYMETA " << " # dentry = depth" << dendl;

			// insert to prefetch list
			vin_phd.push_back(in_phd);
			vvdn_phd.push_back(vdn_phd);

		}
	}

	// lock and check access
	vector<CInode*>::iterator i_it = vin_phd.begin();
	vector<vector<CDentry*>>::iterator dn_it = vvdn_phd.begin();
	while(i_it != vin_phd.end() && dn_it != vvdn_phd.end()){
		in_phd = *i_it;
		vdn_phd = *dn_it;
		// clean up locks
		phd_rdlocks.clear();
		phd_wrlocks.clear();
		phd_xlocks.clear();

		// dentry loss?
		for(int i = 0; i < (int)vdn_phd.size(); i++)
			phd_rdlocks.insert(&vdn_phd[i]->lock);
		mds->locker->include_snap_rdlocks(phd_rdlocks, in_phd);
		mdr->pin(in_phd);
		
		int phd_issued = 0;
		Capability *phd_cap = in_phd->get_client_cap(mdr->get_client());
		if(phd_cap && (mdr->snapid == CEPH_NOSNAP || mdr->snapid <= phd_cap->client_follows))
			phd_issued = phd_cap->issued();
		if ((mask & CEPH_CAP_LINK_SHARED) && !(phd_issued & CEPH_CAP_LINK_EXCL)) phd_rdlocks.insert(&in_phd->linklock);
		if ((mask & CEPH_CAP_AUTH_SHARED) && !(phd_issued & CEPH_CAP_AUTH_EXCL)) phd_rdlocks.insert(&in_phd->authlock);
		if ((mask & CEPH_CAP_XATTR_SHARED) && !(phd_issued & CEPH_CAP_XATTR_EXCL)) phd_rdlocks.insert(&in_phd->xattrlock);
		if ((mask & CEPH_CAP_FILE_SHARED) && !(phd_issued & CEPH_CAP_FILE_EXCL)) {
		    // Don't wait on unstable filelock if client is allowed to read file size.
		    // This can reduce the response time of getattr in the case that multiple
		    // clients do stat(2) and there are writers.
		    // The downside of this optimization is that mds may not issue Fs caps along
		    // with getattr reply. Client may need to send more getattr requests.
		    if (mdr->rdlocks.count(&in_phd->filelock)) {
		      phd_rdlocks.insert(&in_phd->filelock);
		    } else if (in_phd->filelock.is_stable() ||
			       in_phd->filelock.get_num_wrlocks() > 0 ||
			       !in_phd->filelock.can_read(mdr->get_client())) {
		      phd_rdlocks.insert(&in_phd->filelock);
		      mdr->done_locking = false;
		    }
		}
		
		// locking preteched metadata is possible, if failed, no prefetching.
		bool locking = mdr->done_locking;
		if(locking)
			mdr->done_locking = false;
		if(!mds->locker->acquire_locks(mdr, phd_rdlocks, phd_wrlocks, phd_xlocks)){
			dout(2) << " _SPEEDYMETA " << " locking failed." << dendl;
			mdr->done_locking = locking;
			i_it = vin_phd.erase(i_it);
			dn_it = vvdn_phd.erase(dn_it);
			continue;
			// return -1;
		}
		
		if(!check_access(mdr, in_phd, MAY_READ)){
			i_it = vin_phd.erase(i_it);
			dn_it = vvdn_phd.erase(dn_it);
			continue;
		}

		#ifdef _SPEEDYMETA_FEEDBACK
		speedymeta_add_dcscore(ref, path_phd, SCORE_INC);
		#endif
		
		i_it++;
		dn_it++;
	}


	// attach prefetched metadata into metadata request
	mdr->tracei_phd = vin_phd;
	mdr->tracedn_phd = vvdn_phd;

	return 0;
}
#endif

#endif
