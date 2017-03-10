git submodule foreach git pull origin DocDB-merge

xcopy /e GraphViewUnitTest GraphViewUnitTest_bak

xcopy /e GraphView GraphView_bak

xcopy /e _GraphSource\GraphViewUnitTest GraphViewUnitTest

xcopy /e _GraphSource\GraphView GraphView