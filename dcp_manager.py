#!/usr/bin/env python
# coding: utf8

import os
import fnmatch
import argparse
import logging
import datetime
import netrc
from lxml import etree as ET
from email.mime.text import MIMEText
from smtplib import SMTP                    # use this for standard SMTP protocol (TLS encryption)
from email.MIMEText import MIMEText


# -------------------------------
class dcp_manager():
  
  """
  This script is used by the Wieting Theatre (Toledo, IA) to manage the ingest and storage
  of DCP cinema packages, mostly trailers.

  A logfile and a mail can be specified to be informed about operations.
  """

  #--------------------------------
  # fetchPKLAssets
  #
  # Fetch the list of assets from a PKL file.
  #
  # self.pkl - The PKL filename.
  # self.assets - Returned array of assets found. Each asset is another array with an index equal to its 'id',
  # a 'file' component and optional 'annotation'. Note that the PKL file itself MUST be prepended to the list of assets!
  #
  # Returns the AnnotationText value from the PKL or 0 if there was an error.
  #
  def fetchPKLAssets(self):
    from xml.etree import ElementTree
    with open(self.pkl, 'rt') as f:
      tree = ElementTree.parse(f)
      for node in tree.iter():
        # print node.tag, node.attrib
        if node.tag == 'Id':
          id = node.attrib.get('text')
        if node.tag == 'AnnotationText':
          name = node.attrib.get('text')
          
      # Add the PKL file itself to self.assets
      asset = {}
      asset['id'] = id
      asset['file'] = self.pkl
      self.assets.append()

  echo
  "<hr/>";

  // Get
  the
  PKL
  Id.
  if (isset($xml->Id)) {
  $id = (string) $xml->Id;
  
} else {
return FALSE;
}

// Get
the
PKL
name
from its AnnotationText.
if (isset($xml->AnnotationText)) {
$namePKL = (string) $xml->AnnotationText;
} else {
return FALSE;
}

// Seed
the $assets
list
with the PKL file itself.
$assets[$id] = array( );
$assets[$id]['file'] = $path;
$assets[$id]['annotation'] = $namePKL;

$count = 0;

// Logic for PKLs.
if (isset($xml->AssetList)) {
foreach($xml->AssetList->Asset as $a) {
  list($seedDir, $file) = mam_split_path($path);
$asset = array();

// MAM...looks
like
the
asset
filename is held in the
OriginalFileName.
// Id and AnnotationText
may
also
be
significant
so
grab
them!

if (!isset($a->Id)) {
echo "No Id for the file asset '$file'.  It is being skipped.<br>";
mam_log_message(__FUNCTION__, "No Id for the file asset '$file'.  It is being skipped.");
continue;
} else {
$id = (string) $a->Id;
}

if (isset($a->OriginalFileName)) {
$f = (string) $a->OriginalFileName;
$asset['file'] = $seedDir.$f;
$count + +;
} else {
unset($asset[$id]);
echo
"No OriginalFileName for the asset '$file'.  It is being skipped.<br>";
mam_log_message(__FUNCTION__, "No OriginalFileName for the asset '$file'.  It is being skipped.");
continue;
}

if (isset($a->AnnotationText)) {
$asset['annotation'] = (string) $a->AnnotationText;
}

echo
"Found valid $f with an ID of $id.<br>";
mam_log_message(__FUNCTION__, "Found valid $f in PKL $file.");
$assets[$id] = $asset;
}
}

// Count
the $assets
for $id.If less than 3 exist then the package is incomplete and should be deleted.
if ($count < 3) {
$script = "/tmp/dcp_manager.sh";
echo "<br><b>Asset count for $namePKL is only $count.  This PKL is incomplete and should be removed!</b><br>";
$cmd = "unlink $path";
file_put_contents($script, $cmd."\n", FILE_APPEND | LOCK_EX);
}

return $namePKL;
}

    

  # -------------------------------
  # Catalog - Prepare an ASSETMAP file for the self.source directory.
  def catalog(self):
    # Check for an ASSETMAP file in self.dest
    if os.path.exists(self.destAssetMap):
      self.logger.warning("Be advised, the target ASSETMAP file already exists.  It will be replaced.")
      os.remove(self.destAssetMap)
    else:
      self.logger.info("No target " + self.destAssetMap + " found so a new one will be created.")
  
    self.initAssetMap()     # Initialize a new ASSETMAP
    
    # Find all PKL files in the source directory
    pkls = []
    for root, dirnames, filenames in os.walk(self.source):
      for filename in fnmatch.filter(filenames, ['*.PKL.xml', '*.pkl.xml']):
        pkls.append.join(root, filename)

    countPKL = 0
    countAssets = 0

    # Loop on the found PKL files
    for pkl in pkls:
      self.pkl = pkl
      self.assets = []
      name = self.fetchPKLAssets()
      
      each($files as $file) {
    $assets = array();
    $countPKL + +;

    // Fetch
    the
    assets
    for each PKL found.
    $name = fetch_PKL_assets($file, $assets);
    echo "<div style='text-indent:0.5em;' class='prompt'>$name</div>";
    echo "<div style='text-indent:0; margin-top:1em;'>Found PKL file '$file' for examination.</div>";

    $Assets = array( );

    // Pre-scan the list of PKL assets.If any files are missing, issue a warning.

    $complete = TRUE;
    foreach ($assets as $id = > $asset) {
    $Assets[$id] = $asset;
    $file = $asset['file'];
    $basename = basename($file);
    if (!file_exists($file)) {
    echo "<div style='text-indent:3em;'

    class ='warning-message' > Could not find asset '$file'! This package may be incomplete! < / div > ";

    if (!$path = globForAsset($dest, $basename)) {
    echo "<div style='text-indent:3em;'

    class ='error-message' > Could not find asset file '$basename'! This package IS incomplete! < / div > ";
  
    } else {
    echo
    "<div style='text-indent:1em;'>Found target file $basename in $path!</div>";
    $Assets[$id]['file'] = $path.$basename;
  
  }
  } else {
    echo
  "<div style='text-indent:1em;'>Found target file $basename in $dest.</div>";
  $Assets[$id]['file'] = $dest.$basename;
  }
  }


  self.return_value = 0
    return "This is the 'catalog' function.  A new initial ASSETMAP file has been created."


  #------------------------------
  def initAssetMap(self):
    root = ET.Element('AssetMap')
    annotationText = ET.SubElement(root, 'AnnotationText')
    annotationText.text = 'DCP Manager Build ' + self.date
    volumeCount = ET.SubElement(root, 'VolumeCount')
    volumeCount.text = '1'
    issueDate = ET.SubElement(root, 'IssueDate')
    issueDate.text = self.date
    issuer = ET.SubElement(root, 'Issuer')
    issuer.text = 'Mark McFate'
    creator = ET.SubElement(root, 'Creator')
    creator.text = 'DCP Manager'
    assetList = ET.SubElement(root, 'AssetList')
    # print ET.tostring(root, pretty_print=True, xml_declaration=True)
    xml = ET.ElementTree(root)
    xml.write(self.destAssetMap, pretty_print=True, xml_declaration=True)


  #-------------------------------
  #if __name__ == '__main__':
  def load_SMTP_standards(self):
    self.serveraddress = 'summitt.dweller@gmail.com'
    self.SMTPServer = 'smtp.gmail.com'
    self.SMTPPort = 587
    # Read from the .netrc file in your home directory
    secrets = netrc.netrc()
    auth = secrets.authenticators(self.SMTPServer)
    if auth:
      self.SMTPUser = auth[0]
      self.SMTPPassword = auth[2]

  #-------------------------------
  def parse_args(self):
    # Parse arguments
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("OPERATION", help="Specify the DCP Manager operation to perform.")
    parser.add_argument("-s", "--source", help="Specify the directory to perform OPERATION on.  Default is /Volumes/TRAILERS/")
    parser.add_argument("-d", "--destination", help="Specify the destination directory of the OPERATION.  Default is /tmp")
    parser.add_argument("-m", "--mail", help="eMail address where the log file will be sent.  Default is toledowieting@gmail.com")
    parser.add_argument("-l", "--logfile", help="Specify the logfile to record opeartion activity.  Default is /tmp/dcp_manger.log")
    parser.add_argument("--debug", help="Toggle debug output ON.  Default is OFF")
    args = parser.parse_args()

    # Define variables
    self.op = args.OPERATION
    if not args.source:
      self.source = "/Volumes/TRAILERS"
    else:
      self.source = args.source
    if not args.destination:
      self.dest = "/tmp"
    else:
      self.dest = args.destination
    if not args.mail:
      self.mail = "toledowieting@gmail.com"
    else:
      self.mail = args.mail
    if not args.logfile:
      self.logfile = "/tmp/dcp_manger.log"
    else:
      self.logfile = args.logfile
    if not args.debug:
      self.debug = False
    else:
      self.debug = True

    self.args = args
    self.logger = logging.getLogger("logger")
    self.now = datetime.datetime.now()
    self.date = self.now.strftime("%Y-%m-%d")
    
    # Declare the ASSETMAP file(s)
    self.destAssetMap = self.dest + "/ASSETMAP"
    self.sourceAssetMap = self.source + "/ASSETMAP"

    # Logging
    FORMAT = '%(asctime)-15s (%(levelname)s): %(message)s'
    if self.logfile and self.mail:
      if self.check_dir_exist(self.logfile):
        os.remove(self.logfile)
      if not self.debug:
        logging.basicConfig(filename=self.logfile, filemode='w', level=logging.INFO, format=FORMAT, datefmt='%Y.%m.%d %H:%M:%S')
      else:
        logging.basicConfig(filename=self.logfile, filemode='w', level=logging.DEBUG, format=FORMAT, datefmt='%Y.%m.%d %H:%M:%S')
      consoleHandler = logging.StreamHandler()
      console_format = logging.Formatter(FORMAT)
      consoleHandler.setFormatter(console_format)
      consoleHandler.setLevel(logging.DEBUG) if self.debug else consoleHandler.setLevel(logging.INFO)
      self.logger.addHandler(consoleHandler)

    # Verify source directory exists
    if not self.check_dir_exist(self.source):
      msg = "Source directory " + self.source + " does not exist.  Exiting..."
      self.logger.error(msg)
      if self.mail:
        self.send_mail(self.mail, self.logfile, 1, msg)
        exit(1)

    # Verify destination directory exists
    if not self.check_dir_exist(self.dest):
      msg = "Destination directory " + self.dest + " does not exist.  Exiting..."
      self.logger.error(msg)
      if self.mail:
        self.send_mail(self.mail, self.logfile, 1, msg)
        exit(1)

  # -------------------------------
  # Function to determine if a directory exists
  def check_dir_exist(self, os_dir):
    if os.path.exists(os_dir):
      self.logger.debug("{} exists.".format(os_dir))
      return True
    else:
      self.logger.warning("{} does not exist.".format(os_dir))
      return False

  #-------------------------------
  # Mail mit Log senden
  # Params: Recipient, Logfile, Returncode, Output
  def send_mail(self, recipient, logfile, return_value, output):
    self.load_SMTP_standards()
    if self.mail:
      if logfile:
        # Open the logfile for reading.
        fp = open(self.logfile, 'rb')
        # Create a text/plain message
        msg = MIMEText(fp.read())
        fp.close()
      elif output:
        msg = MIMEText(output)
      else:
        msg = MIMEText("The " + self.op + " operation is done.")
      if return_value == 0:
        msg['Subject'] = "The " + self.op + " operation has completed successfully."
      else:
        msg['Subject'] = "The " + self.op + " operation did NOT succeed! An Error occured! Return Code = " + str(return_value) + "."
      msg['From'] = self.serveraddress
      msg['To'] = recipient

      try:
        self.conn = SMTP(self.SMTPServer, self.SMTPPort)
        self.conn.set_debuglevel(False)
        # Starting an encrypted TLS-Session
        self.conn.starttls()
        self.conn.login(self.SMTPUser, self.SMTPPassword)
        self.conn.sendmail(self.serveraddress, [recipient], msg.as_string())
        self.logger.info("Successfully sent mail to %s." % recipient)
      except Exception as e:
        self.logger.error("Sending mail failed! Error: %s." % e)
      finally:
        self.conn.close()


  #-------------------------------
  # Perform the specified OPERATION !!!!!!!!!!!!!!!!!!!!!!!!!!!!!
  def main(self):
    self.parse_args()
    self.return_value = 0
    if self.logfile:
      self.logger.info("Saving " + self.op + " operation logfile to " + self.logfile + ".")
    else:
      self.logger.info("Starting " + self.op + " operation without a logfile.")
      
    # The operation switch...
    if self.op.lower() == 'catalog':
      out = self.catalog()
    else:
      msg = "The specifed '" + self.op + "' is NOT supported.  Exiting..."
      self.logger.info(msg)
      if self.mail:
        self.send_mail(self.mail, self.logfile, 400, msg)
      exit(1)
      
    self.logger.info("Operation " + self.op + " is complete with a return code " + str(self.return_value) + ".")
    if self.mail:
      self.send_mail(self.mail, self.logfile, self.return_value, out)


# -------------------------------
# Create and call the main()
x = dcp_manager();
x.main();