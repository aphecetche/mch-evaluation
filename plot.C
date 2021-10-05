#include "MergeableCollection.h"

void plot(int rebin=100) {

TFile f("preclusters.stats.root");

o2::mch::eval::MergeableCollection* HC = 
static_cast<o2::mch::eval::MergeableCollection*>(f.Get("HC"));

TH1* hdc = (TH1*)(HC->histo("/DIGITS/ChargePerTimeBin")->Clone("hdc"));
TH1* hdn = (TH1*)(HC->histo("/DIGITS/NofDigitsPerTimeBin")->Clone("hdn"));
TH1* hcn = (TH1*)(HC->histo("/PRECLUSTERS/NofPreClustersPerTimeBin")->Clone("hcn"));
TH1* hcc = (TH1*)(HC->histo("/PRECLUSTERS/ChargePerTimeBin")->Clone("hcc"));

hdn->SetDirectory(nullptr);
hdc->SetDirectory(nullptr);
hcn->SetDirectory(nullptr);
hcc->SetDirectory(nullptr);

hdn->Rebin(rebin);
hdc->Rebin(rebin);
hcn->Rebin(rebin);
hcc->Rebin(rebin);

TCanvas* c1 = new TCanvas;
c1->Divide(2,2);

c1->cd(1);
hdn->Draw("hist");
c1->cd(2);
hdc->Draw("hist");

c1->cd(3);
hcn->Draw("hist");
c1->cd(4);
hcc->Draw("hist");

}
