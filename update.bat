git submodule foreach git pull origin DocDB-merge

rmdir /s GraphViewUnitTest_bak

rmdir /s GraphView_bak

xcopy /e GraphViewUnitTest GraphViewUnitTest_bak

xcopy /e GraphView GraphView_bak

rmdir /s GraphViewUnitTest

rmdir /s GraphView

xcopy /e _GraphSource\GraphViewUnitTest GraphViewUnitTest

xcopy /e _GraphSource\GraphView GraphView