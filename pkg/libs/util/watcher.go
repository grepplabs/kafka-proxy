package util

import (
	"github.com/fsnotify/fsnotify"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"os"
	"path/filepath"
	"time"
)

func WatchForUpdates(filename string, done <-chan bool, action func()) error {
	symlink, err := isSymLink(filename)
	if err != nil {
		return err
	}
	if symlink {
		return watchLinkForUpdates(filename, done, action)
	}
	return watchFileForUpdates(filename, done, action)
}

func isSymLink(filename string) (bool, error) {
	fileInfo, err := os.Lstat(filename)
	if err != nil {
		return false, err
	}

	if fileInfo.Mode()&os.ModeSymlink == os.ModeSymlink {
		return true, nil
	}
	return false, nil
}

func waitForReplacement(filename string, op fsnotify.Op, watcher *fsnotify.Watcher) {
	const sleep_interval = 50 * time.Millisecond

	// Avoid a race when fsnofity.Remove is preceded by fsnotify.Chmod.
	if op&fsnotify.Chmod != 0 {
		time.Sleep(sleep_interval)
	}
	for {
		if _, err := os.Stat(filename); err == nil {
			if err := watcher.Add(filename); err == nil {
				logrus.Debugf("watching resumed for %s", filename)
				return
			}
		}
		time.Sleep(sleep_interval)
	}
}

func watchFileForUpdates(filename string, done <-chan bool, action func()) error {
	filename = filepath.Clean(filename)
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return errors.Wrapf(err, "failed to create watcher for %s", filename)
	}
	go func() {
		defer func() {
			if r := recover(); r != nil {
				var ok bool
				err, ok := r.(error)
				if ok {
					logrus.Errorf("watch file %s for update error %v", filename, err)
				}
			}
		}()
		defer watcher.Close()
	done:
		for {
			select {
			case <-done:
				logrus.Printf("Shutting down watcher for: %s", filename)
				break done
			case event := <-watcher.Events:
				if event.Op&(fsnotify.Remove|fsnotify.Rename|fsnotify.Chmod) != 0 {
					logrus.Debugf("watching interrupted on event: %s", event)
					err := watcher.Remove(filename)
					if err != nil {
						logrus.Debugf("failed to remove %s: %s", filename, err)
					}
					waitForReplacement(filename, event.Op, watcher)
				}
				logrus.Infof("execute action after event %s on %s ", event.Op, event.Name)
				action()
			case err := <-watcher.Errors:
				logrus.Printf("error watching %s: %s", filename, err)
			}
		}
	}()
	if err = watcher.Add(filename); err != nil {
		return errors.Wrapf(err, "failed to add %s watcher", filename)
	}
	logrus.Printf("watching %s for updates", filename)
	return nil
}

func watchLinkForUpdates(filename string, done <-chan bool, action func()) error {
	filename = filepath.Clean(filename)
	dirname := filepath.Dir(filename)
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return errors.Wrapf(err, "failed to create watcher for file %s in dir %s", filename, dirname)
	}
	go func() {
		defer func() {
			if r := recover(); r != nil {
				var ok bool
				err, ok := r.(error)
				if ok {
					logrus.Errorf("watch link %s for update error %v", filename, err)
				}
			}
		}()
		defer watcher.Close()
	done:
		for {
			select {
			case <-done:
				logrus.Printf("Shutting down watcher for: %s", dirname)
				break done
			case event := <-watcher.Events:
				if event.Op&(fsnotify.Remove|fsnotify.Rename|fsnotify.Chmod) != 0 {
					logrus.Debugf("watching interrupted on event: %s", event)
					err := watcher.Remove(dirname)
					if err != nil {
						logrus.Debugf("failed to remove %s: %s", dirname, err)
					}
					waitForReplacement(dirname, event.Op, watcher)
				}
				if event.Op&(fsnotify.Remove) != 0 {
					continue
				}
				targetname, err := filepath.EvalSymlinks(filename)
				if err != nil {
					logrus.Printf("error evaluating target symlink %s: %s", targetname, err)
					continue
				}
				name, err := filepath.EvalSymlinks(event.Name)
				if err != nil {
					logrus.Printf("error evaluating event symlink %s: %s", event.Name, err)
					continue
				}
				if name == targetname {
					logrus.Infof("execute action after event %s on %s ", event.Op, event.Name)
					action()
				} else {
					logrus.Debugf("skip action after event: %v", event)
				}
			case err := <-watcher.Errors:
				logrus.Printf("error watching %s: %s", dirname, err)
			}
		}
	}()
	if err = watcher.Add(dirname); err != nil {
		return errors.Wrapf(err, "failed to add %s watcher", dirname)
	}
	logrus.Infof("watching %s for updates on link %s", dirname, filename)
	return nil
}
