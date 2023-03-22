#!/bin/sh

nohup dstest -r -p 32 -n 100000 -v 2A TestBasicAgree2B TestRPCBytes2B TestFailAgree2B TestFailNoAgree2B TestConcurrentStarts2B TestRejoin2B TestBackup2B TestCount2B &
nohup dstest -r -p 32 -n 100000 -v TestSnapshotBasic2D TestSnapshotInstall2D TestSnapshotInstallUnreliable2D TestSnapshotInstallCrash2D TestSnapshotInstallUnCrash2D &
nohup dstest -r -p 64 -n 100000 -v TestPersist12C TestPersist22C TestPersist32C TestFigure82C TestUnreliableAgree2C TestReliableChurn2C TestUnreliableChurn2C &
