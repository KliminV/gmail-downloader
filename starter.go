package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"golang.org/x/time/rate"
	"google.golang.org/api/gmail/v1"
	"google.golang.org/api/option"
)

var appSettings Setting

type Setting struct {
	useCompression bool
	useSaved       bool
	dataFolder     string
	maxRate        int64
	debug          bool
	minTreshold    int
}
type EmailInfo struct {
	Subject string
	Size    int64
	Sender  string
}

func getClient(config *oauth2.Config) *http.Client {
	tokFile := "secrets/token.json"
	tok, err := tokenFromFile(tokFile)
	if err != nil {
		tok = getTokenFromWeb(config)
		saveToken(tokFile, tok)
	}
	return config.Client(context.Background(), tok)
}

// Request a token from the web, then returns the retrieved token.
func getTokenFromWeb(config *oauth2.Config) *oauth2.Token {
	authURL := config.AuthCodeURL("state-token", oauth2.AccessTypeOffline)
	log.Printf("Go to the following link in your browser then type the "+
		"authorization code: \n%v\n", authURL)

	var authCode string
	if _, err := fmt.Scan(&authCode); err != nil {
		log.Fatalf("Unable to scan authorization code: %v", err)
	}

	tok, err := config.Exchange(context.TODO(), authCode)
	if err != nil {
		log.Fatalf("Unable to retrieve token from web: %v", err)
	}
	return tok
}

// Retrieves a token from a local file.
func tokenFromFile(file string) (*oauth2.Token, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	tok := &oauth2.Token{}
	err = json.NewDecoder(f).Decode(tok)
	return tok, err
}

func saveToken(path string, token *oauth2.Token) {
	log.Printf("Saving credential file to: %s\n", path)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		log.Fatalf("Unable to cache oauth token: %v", err)
	}
	defer f.Close()
	json.NewEncoder(f).Encode(token)
}

func client() *http.Client {
	b, err := os.ReadFile("secrets/credentials.json")
	if err != nil {
		log.Fatalf("Unable to read client secret file: %v", err)
	}

	config, err := google.ConfigFromJSON(b, gmail.GmailReadonlyScope)
	if err != nil {
		log.Fatalf("Unable to parse client secret file to config: %v", err)
	}
	return getClient(config)
}
func processCompressedEmailFile(filePath string) (EmailInfo, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return EmailInfo{}, err
	}
	defer file.Close()

	gz, err := gzip.NewReader(file)
	if err != nil {
		return EmailInfo{}, err
	}
	defer gz.Close()

	var msg gmail.Message
	decoder := json.NewDecoder(gz)
	err = decoder.Decode(&msg)
	if err != nil {
		return EmailInfo{}, err
	}

	var subject, sender string

	for _, header := range msg.Payload.Headers {
		if header.Name == "Subject" {
			subject = header.Value
		}
		if header.Name == "From" {
			sender = header.Value
		}
	}

	emailInfo := EmailInfo{
		Subject: subject,
		Size:    msg.SizeEstimate,
		Sender:  sender,
	}

	return emailInfo, nil
}

func readEmails(folderPath string) ([]EmailInfo, error) {
	files, err := os.ReadDir(folderPath)
	if err != nil {
		log.Fatal("Cant read emails")
		return nil, err
	}

	var emailList []EmailInfo

	for _, file := range files {
		if !file.IsDir() {
			filePath := filepath.Join(folderPath, file.Name())
			var emailInfo EmailInfo
			if appSettings.useCompression && filepath.Ext(file.Name()) == ".gz" {

				emailInfo, err = processCompressedEmailFile(filePath)
			} else if !appSettings.useCompression && filepath.Ext(file.Name()) == ".json" {
				emailInfo, err = processEmailFile(filePath)
			} else {
				continue
			}

			if err != nil {
				log.Printf("Error processing file %s: %v", file.Name(), err)
				continue // Skip to the next file
			}
			emailList = append(emailList, emailInfo)
		}
	}
	return emailList, nil
}

func processEmailFile(filePath string) (EmailInfo, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return EmailInfo{}, err
	}
	defer file.Close()

	var msg gmail.Message
	decoder := json.NewDecoder(file)
	err = decoder.Decode(&msg)
	if err != nil {
		return EmailInfo{}, err
	}

	var subject, sender string

	for _, header := range msg.Payload.Headers {
		if header.Name == "Subject" {
			subject = header.Value
		}
		if header.Name == "From" {
			sender = header.Value
		}
	}

	emailInfo := EmailInfo{
		Subject: subject,
		Size:    msg.SizeEstimate,
		Sender:  sender,
	}

	return emailInfo, nil
}

func saveMessages(srv *gmail.Service, n int64, folder string, file string) {
	user := "me"
	limiter := rate.NewLimiter(rate.Limit(appSettings.maxRate), 2)
	var wg sync.WaitGroup
	var nextPageToken string
	for {
		call := srv.Users.Messages.List(user).MaxResults(n)
		if nextPageToken != "" {
			call = call.PageToken(nextPageToken)
		}

		r, err := call.Do()
		if err != nil {
			log.Fatalf("Unable to retrieve messages: %v", err)
		}

		if len(r.Messages) == 0 {
			break
		}

		for _, m := range r.Messages {
			wg.Add(1)
			go func(msgID string) {
				defer wg.Done()
				err := limiter.Wait(context.Background())
				if err != nil {
					log.Printf("Unable to wait limit %v: %v", msgID, err)
					return
				}

				msg, err := srv.Users.Messages.Get(user, msgID).Do()
				if err != nil {
					log.Printf("Unable to retrieve message %v: %v", msgID, err)
					return
				}

				err = saveEmail(msg, folder)
				if err != nil {
					log.Printf("Unable to save message %s: %v", msg.Id, err)
					return
				}
				if appSettings.debug {
					if appSettings.useCompression {
						log.Printf("Saved compressed message: %s.gz\n", msg.Id)
					} else {
						log.Printf("Saved message: %s\n", msg.Id)
					}
				}
			}(m.Id)
		}

		nextPageToken = r.NextPageToken
		if nextPageToken == "" {
			break
		}
	}
	wg.Wait()
}

func saveEmail(msg *gmail.Message, folder string) error {
	baseFilename := fmt.Sprintf("email_%s", msg.Id)             // Only msg.Id
	fullFilename := filepath.Join("data", baseFilename+".json") // Add .json
	if appSettings.useCompression {
		fullFilename += ".gz"
		var buf bytes.Buffer
		gz := gzip.NewWriter(&buf)

		encoder := json.NewEncoder(gz)
		if err := encoder.Encode(msg); err != nil {
			return err
		}

		if err := gz.Close(); err != nil {
			return err
		}

		return os.WriteFile(fullFilename, buf.Bytes(), 0644)
	} else {
		file, err := os.Create(fullFilename)
		if err != nil {
			return err
		}
		defer file.Close()

		encoder := json.NewEncoder(file)
		if err := encoder.Encode(msg); err != nil {
			return err
		}
		return nil
	}
}

func aggregateBySender(emails []EmailInfo) map[string][]EmailInfo {
	senderMap := make(map[string][]EmailInfo)
	for _, email := range emails {
		senderMap[email.Sender] = append(senderMap[email.Sender], email)
	}
	return senderMap
}

func sortSendersByCount(senderMap map[string][]EmailInfo) []string {
	senders := make([]string, 0, len(senderMap))
	for sender := range senderMap {
		senders = append(senders, sender)
	}

	sort.Slice(senders, func(i, j int) bool {
		return len(senderMap[senders[i]]) < len(senderMap[senders[j]])
	})

	return senders
}

func main() {

	appSettings = Setting{
		useCompression: true,
		useSaved:       true,
		dataFolder:     "data/",
		maxRate:        50,
		debug:          false,
		minTreshold:    25,
	}

	ctx := context.Background()
	client := client()

	srv, err := gmail.NewService(ctx, option.WithHTTPClient(client))
	if err != nil {
		log.Fatalf("Unable to retrieve Gmail client: %v", err)
	}

	if !appSettings.useSaved {
		saveMessages(srv, 10000, appSettings.dataFolder, "email_%s.json")
	}

	emailList, err := readEmails(appSettings.dataFolder)
	if err != nil {
		log.Fatalf("Error reading emails: %v", err)
	}

	if appSettings.debug {
		for _, email := range emailList {
			log.Printf("Subject: %s, Size: %d, Sender: %s\n", email.Subject, email.Size, email.Sender)
		}
	}

	senderAggregated := aggregateBySender(emailList)
	sortedSenders := sortSendersByCount(senderAggregated)

	log.Printf("Emails aggregated by sender:")
	for _, sender := range sortedSenders {
		emails := senderAggregated[sender]
		if len(emails) > appSettings.minTreshold {
			log.Printf("Sender: %s, Count: %d\n", sender, len(emails))
		}
	}

}
