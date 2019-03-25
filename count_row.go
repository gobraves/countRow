package main

import (
	"encoding/json"
	"flag"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"io"
	"io/ioutil"
	"log"
	"os"
	"regexp"
	"sync"
	"time"
)

type FileTypeEnum int

const (
	Single FileTypeEnum = iota
	Multipart
)

type TableRecordNum struct {
	Mu        sync.RWMutex
	TableDict map[string]int
}

type SqlFile struct {
	Content *[]byte
	Name    string
	Type    FileTypeEnum

	// 从0开始计数
	FilePartIndex int
	Start         int
	End           int
}

func makeTableRecord() *TableRecordNum {
	t := &TableRecordNum{}
	t.TableDict = make(map[string]int)
	return t
}

var (
	endpoint        string
	bucketName      string
	awsLinkNum      int
	CountWorker     int
	region          string
	singleFileLimit int
	prefix string
)

func init() {
	flag.StringVar(&endpoint, "endpoint", "", "e.g.: -endpoint 192.168.1.1")
	flag.StringVar(&bucketName, "bucket_name", "", "e.g.: -bucket_name tidb")
	flag.IntVar(&awsLinkNum, "aws_link_number", 100, "e.g.: -aws_link_number 100")
	flag.IntVar(&CountWorker, "count_worker", 10, "e.g. -count_worker 10")
	flag.IntVar(&singleFileLimit, "single_file_limit", 10*1024*1024, "单位为字节")
	flag.StringVar(&region, "aws_region", os.Getenv("AWS_DEFAULT_REGION"), "")
	flag.StringVar(&prefix, "prefix", "", "")

	file, err := os.OpenFile("count_row.log", os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalln("fail to create count_row.log file!")
	}
	mw := io.MultiWriter(os.Stdout, file)
	log.SetOutput(mw)
}

func main() {
	log.Println("开始解析参数")
	flag.Parse()

	log.Println("配置aws")
	sess, err := session.NewSession(&aws.Config{
		Region:   aws.String(region),
		Endpoint: aws.String(endpoint),
	})

	if err != nil{
		log.Fatal("aws 连接失败")
	}

	svc := s3.New(sess)

	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(prefix),
	}

	tableState := makeTableRecord()

	tableListCh := make(chan string, 1000)
	tableCh := make(chan SqlFile, 1000)

	var wg sync.WaitGroup

	t1 := time.Now().UnixNano() / 1e6
	log.Println("任务开始执行")

	// 获取table列表
	getFlag := true
	for {
		if getFlag {
			getFlag = getTableList(svc, input, tableListCh, &wg)
		} else {
			close(tableListCh)
			break
		}

	}
	// 获取table数据
	for i := 0; i < awsLinkNum; i++ {
		go downloadWorker(bucketName, svc, tableCh, tableListCh, &wg)
	}

	// 开始处理sql文件
	for i := 0; i < CountWorker; i++ {
		go countWorker(tableState, tableCh, &wg)
	}

	wg.Wait()

	resJson, _ := json.Marshal(tableState.TableDict)

	f, err := os.OpenFile("count_row_result", os.O_CREATE | os.O_RDWR,0666)
	_, err = f.Write(resJson)
	if err != nil {
		log.Println("写入文件失败")
	}
	_ = f.Close()
	log.Println(string(resJson))

	t2 := time.Now().UnixNano() / 1e6
	log.Printf("任务执行结束，共耗时:%.3fs", float32(t2-t1)/1000)
}

func splitFile(sqlFile *SqlFile) ([]int, int) {
	//将文件按换行符划分
	//先按设定singleFileLimit，划分为几块
	//然后从划分的位置开始，从右往左找换行符
	//posArr存储的是划分数组的节点数据
	runeContent := []rune(string(*sqlFile.Content))
	contentLen := len(runeContent)
	nums := contentLen / singleFileLimit

	log.Printf("文件:%s 大小:%d 划分了:%d块 \n", sqlFile.Name, len(runeContent), nums+1)

	posArr := make([]int, nums+2)

	posArr[0] = 0

	for i := 0; i < nums; i++ {
		pos := findNewLineChar(&runeContent, i*singleFileLimit, (i+1)*singleFileLimit)
		posArr[i+1] = pos
	}

	//最后的划分节点为文件的末尾，即使文件长度刚好被singleFileLimit整除也并不影响
	posArr[nums+1] = len(runeContent)
	return posArr, nums + 1
}

func findNewLineChar(fileContent *[]rune, left int, right int) int {
	// 从右往左找换行符
	pos := right
	for i := right; i > left; i-- {
		// 10 在utf-8编码中为“\n"
		if (*fileContent)[i] == 10 {
			pos = i
			break
		}
	}
	return pos
}

func getTableList(svc *s3.S3, input *s3.ListObjectsV2Input, tableListCh chan string, wg *sync.WaitGroup) bool {
	result, err := svc.ListObjectsV2(input)
	getFlag := true
	if err == nil {
		for _, content := range result.Contents {
			if *content.Key == "employees.departments1.sql" {
				wg.Add(1)
				tableListCh <- *content.Key
			}
		}

		if *result.IsTruncated == true {
			input.ContinuationToken = result.NextContinuationToken
		} else {
			getFlag = false
		}
	} else {
		getFlag = false
		log.Panicf("文件列表获取失败， ContinuationToken: %s",input.ContinuationToken)
	}

	return getFlag
}

func downloadWorker(bucket string, svc *s3.S3, tableCh chan<- SqlFile, tableListCh <-chan string, wg *sync.WaitGroup) {

	for tableName := range (tableListCh) {
		log.Printf("正在下载:%s", tableName)
		t1 := time.Now().UnixNano() / 1e6

		inputGetObject := &s3.GetObjectInput{
			Key:    aws.String(tableName),
			Bucket: aws.String(bucket),
		}
		res, err := svc.GetObject(inputGetObject)

		if err == nil {
			content, _ := ioutil.ReadAll(res.Body)

			f := SqlFile{
				Content: &content,
				Name:    tableName,
			}

			t2 := time.Now().UnixNano() / 1e6
			log.Printf("文件:%s已下载完成, 大小:%d 共耗时:%.3fs\n", tableName, len(content), float32((t2-t1)/1e3))

			if len(*f.Content) <= singleFileLimit {
				f.Type = 0
				f.FilePartIndex = 0
				f.Start = 0
				f.End = len(*f.Content)
				tableCh <- f
				wg.Add(1)
			} else {
				f.Type = 1
				arr, partNum := splitFile(&f)

				wg.Add(partNum)
				// nums+1，代表nums+1个数据块
				for i := 1; i <= partNum; i++ {
					tmpSqlFile := SqlFile{
						Type:          FileTypeEnum(1),
						Name:          f.Name,
						FilePartIndex: i - 1,
						Content:       f.Content,
						Start:         arr[i-1],
						End:           arr[i],
					}

					tableCh <- tmpSqlFile
				}
			}
		} else {
			log.Printf("%s 下载失败", tableName)
		}
		wg.Done()
	}
}

func countWorker(tableState *TableRecordNum, tableCh <-chan SqlFile, wg *sync.WaitGroup) {

	for file := range (tableCh) {
		log.Println("正在处理文件:", file.Name+"-", file.FilePartIndex)

		table := file.Name
		t1 := time.Now().UnixNano() / 1e6

		pattern, err:= regexp.Compile(`(?m)^\(.*\)(,|;)?$`)
		if err != nil {
			log.Panicf("%s-%d 正则匹配失败", file.Name, file.FilePartIndex)
		}

		s := (*file.Content)[file.Start:file.End]
		matchByteArr := pattern.FindAll(s, -1)

		matchNum := len(matchByteArr)

		t2 := time.Now().UnixNano() / 1e6

		log.Printf("文件:%s-%d已处理完成 fileType: %d, 共耗时:%.3fs\n", file.Name, file.FilePartIndex,
			file.Type, float32((t2-t1)/1e3))

		tableState.Mu.Lock()

		if _, ok := tableState.TableDict[table]; ok {
			tableState.TableDict[table] += matchNum
		} else {
			tableState.TableDict[table] = matchNum
		}

		tableState.Mu.Unlock()
		wg.Done()
	}
}
