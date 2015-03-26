package backup

import (
	"time"

	"github.com/soundtrackyourbrand/utils/gae"

	"appengine"
	"appengine/datastore"
)

const (
	AEBackupInformationKind = "_AE_Backup_Information"
)

type Backup struct {
	Id            *datastore.Key `datastore:"-"`
	CompleteTime  time.Time      `datastore:"complete_time"`
	CompletedJobs []string       `datastore:"completed_jobs"`
	Filesystem    string         `datastore:"filesystem"`
	GSHandle      string         `datastore:"gs_handle"`
	Kinds         []string       `datastore:"kinds"`
	Name          string         `datastore:"name"`
	StartTime     time.Time      `datastore:"start_time"`
}

type Backups []*Backup

func GetBackups(c appengine.Context) (result Backups, err error) {
	ids, err := datastore.NewQuery(AEBackupInformationKind).GetAll(c, &result)
	err = gae.FilterOkErrors(err)
	if err != nil {
		return
	}
	for index, id := range ids {
		result[index].Id = id
	}
	return
}
