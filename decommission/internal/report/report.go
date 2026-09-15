// Package report renders erasure reports and data erasure certificates.
// Each verified disk gets two immutable objects in MinIO (or the FS backend):
//
//	reports/{certNo}.json  — machine readable full report
//	reports/{certNo}.html — printable erasure certificate (可打印归档)
package report

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"html/template"
	"strings"
	"time"

	"idc/decommission/internal/domain"
)

// FullReport is the machine-readable record archived per disk.
type FullReport struct {
	Schema      string              `json:"schema"`
	Certificate domain.Certificate  `json:"certificate"`
	Asset       domain.Asset        `json:"asset"`
	Disk        domain.Disk         `json:"disk"`
	Runs        []domain.ErasureRun `json:"runs"`
	GeneratedAt time.Time           `json:"generated_at"`
}

// Build composes the stored objects from the aggregated entities and returns
// (jsonBytes, htmlBytes, keys).
func Build(cert domain.Certificate, asset domain.Asset, disk domain.Disk, runs []domain.ErasureRun) (jsonOut, htmlOut []byte, reportKey, certKey string, err error) {
	rep := FullReport{
		Schema: "idc-erasure-report/v1", Certificate: cert,
		Asset: asset, Disk: disk, Runs: runs, GeneratedAt: time.Now().UTC(),
	}
	jsonOut, err = json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return nil, nil, "", "", err
	}
	reportKey = fmt.Sprintf("reports/%s.json", cert.CertNo)
	certKey = fmt.Sprintf("reports/%s.html", cert.CertNo)

	var htmlBuf bytes.Buffer
	if err := certTpl.Execute(&htmlBuf, tplData{Report: rep, JSON: string(jsonOut)}); err != nil {
		return nil, nil, "", "", err
	}
	htmlOut = htmlBuf.Bytes()
	return jsonOut, htmlOut, reportKey, certKey, nil
}

// SHA256Hex is a convenience used by callers storing the digest.
func SHA256Hex(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

type tplData struct {
	Report FullReport
	JSON   string
}

var funcMap = template.FuncMap{
	"passes": func(ps []domain.PassSpec) string {
		out := make([]string, len(ps))
		for i, p := range ps {
			out[i] = fmt.Sprintf("%d:%s", p.Index+1, p.Pattern)
		}
		return strings.Join(out, ", ")
	},
}

var certTpl = template.Must(template.New("cert").Funcs(funcMap).Parse(`<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<title>数据擦除证明 {{.Report.Certificate.CertNo}}</title>
<style>
 body{font-family:"Noto Sans CJK SC","Microsoft YaHei",sans-serif;max-width:860px;margin:32px auto;padding:0 24px;color:#1a1a1a}
 h1{text-align:center;letter-spacing:6px;margin-bottom:0}
 .sub{text-align:center;color:#666;margin:6px 0 28px}
 table{border-collapse:collapse;width:100%;margin:14px 0;font-size:14px}
 th,td{border:1px solid #999;padding:7px 10px;text-align:left}
 th{background:#f2f4f7;width:28%}
 .seal{border:3px double #c00;border-radius:10px;color:#c00;display:inline-block;padding:10px 18px;
       transform:rotate(-12deg);font-weight:bold;letter-spacing:4px;margin-top:10px}
 .sign{margin-top:48px;display:flex;justify-content:space-between}
 .muted{color:#777;font-size:12px}
 pre{background:#f6f6f6;padding:10px;font-size:11px;overflow:auto;max-height:260px;border:1px solid #ddd}
 h2{font-size:16px;border-left:4px solid #335;padding-left:8px;margin-top:28px}
</style></head>
<body>
<h1>数据擦除证明</h1>
<div class="sub">DATA ERASURE CERTIFICATE · {{.Report.Certificate.CertNo}}</div>

<table>
<tr><th>证明编号</th><td>{{.Report.Certificate.CertNo}}</td><th>签发时间(UTC)</th><td>{{.Report.Certificate.CreatedAt.Format "2006-01-02 15:04:05"}}</td></tr>
<tr><th>资产编号</th><td>{{.Report.Certificate.AssetTag}}</td><th>设备序列号</th><td>{{.Report.Asset.SN}}</td></tr>
<tr><th>设备型号</th><td>{{.Report.Asset.Vendor}} {{.Report.Asset.Model}}</td><th>机房/机柜</th><td>{{.Report.Asset.Room}} / {{.Report.Asset.Rack}}</td></tr>
<tr><th>磁盘序列号</th><td>{{.Report.Certificate.DiskSerial}}</td><th>磁盘型号/类型</th><td>{{.Report.Certificate.DiskModel}} / {{.Report.Disk.Kind}}</td></tr>
<tr><th>容量(GB)</th><td>{{.Report.Certificate.CapacityGB}}</td><th>任务编号</th><td>{{.Report.Certificate.JobID}}</td></tr>
</table>

<h2>擦除方式</h2>
<table>
<tr><th>执行标准</th><td>{{.Report.Certificate.StandardName}}（{{.Report.Certificate.Standard}}）</td></tr>
<tr><th>覆写道次</th><td>{{passes .Report.Certificate.Passes}}</td></tr>
<tr><th>复验方式</th><td>{{if eq .Report.Certificate.VerifyMode "full"}}全盘逐字节复验{{else}}抽样复验{{end}}（复验字节数 {{.Report.Certificate.VerifyBytes}}）</td></tr>
<tr><th>开始时间(UTC)</th><td>{{.Report.Certificate.StartedAt.Format "2006-01-02 15:04:05"}}</td></tr>
<tr><th>完成时间(UTC)</th><td>{{.Report.Certificate.FinishedAt.Format "2006-01-02 15:04:05"}}</td></tr>
<tr><th>擦除次数(含重擦)</th><td>{{.Report.Certificate.Attempts}}</td></tr>
<tr><th>报告文件</th><td>{{.Report.Certificate.ReportKey}}<br><span class="muted">SHA256: {{.Report.Certificate.ReportSHA256}}</span></td></tr>
</table>

<h2>执行记录</h2>
<table>
<tr><th>#</th><th>开始</th><th>结束</th><th>断点续擦</th><th>覆写字节</th><th>复验字节</th><th>结果</th><th>说明</th></tr>
{{range $i,$r := .Report.Runs}}<tr>
<td>{{$i}}</td>
<td>{{$r.StartedAt.Format "2006-01-02 15:04:05"}}</td>
<td>{{$r.FinishedAt.Format "2006-01-02 15:04:05"}}</td>
<td>{{if $r.Resumed}}是{{else}}否{{end}}</td>
<td>{{$r.WroteBytes}}</td>
<td>{{$r.VerifyBytes}}</td>
<td>{{$r.Result}}</td>
<td>{{$r.Detail}}</td>
</tr>{{end}}
</table>

<p>兹证明上述磁盘承载的数据已按照所标注标准完成全盘覆写，并经字节级复验确认不可恢复，符合退役介质处置要求。</p>

<div class="sign">
 <div>擦除操作人：{{.Report.Certificate.Operator}}<br><br>日期：________________</div>
 <div>安全负责人：________________<br><br>日期：________________</div>
 <div class="seal">已擦除 · VERIFIED</div>
</div>

<h2>机器可读报告（存档 JSON 摘要）</h2>
<pre>{{.JSON}}</pre>
</body></html>`))
