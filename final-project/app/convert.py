import csv, os.path

dat_files = ['artists.dat',
      'tags.dat',
      'user_artists.dat',
      'user_friends.dat',
      'user_taggedartists-timestamps.dat',
      'user_taggedartists.dat']

csv_files = ['artists.csv',
      'tags.csv',
      'user_artists.csv',
      'user_friends.csv',
      'user_taggedartists-timestamps.csv',
      'user_taggedartists.csv']

for i in range(len(dat_files)):
  if not os.path.exists('lastfm-dataset/csv/'):
    os.makedirs('lastfm-dataset/csv/')
  with open('lastfm-dataset/' + dat_files[i], encoding="ISO-8859-1") as dat_file, open('lastfm-dataset/csv/' + csv_files[i], 'w') as csv_file:
    csv_writer = csv.writer(csv_file)
    for line in dat_file:
      row = [field.strip() for field in line.split('\t')]
      csv_writer.writerow(row)