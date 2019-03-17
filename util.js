module.exports = {
    getCurrentDateTime: function (){
         var today = new Date();
         var dd = today.getDate();
         var mm = today.getMonth() + 1;
         var yyyy = today.getFullYear();
         var hours = today.getHours();
         var mins = today.getMinutes();
         var seconds = today.getSeconds();
     
         if (dd < 10) dd = '0' + dd;
         if (mm < 10) mm = '0' + mm;
         if (hours < 10) hours = '0' + hours;
         if (mins < 10) mins = '0' + mins;
         if (seconds < 10) seconds = '0' + seconds;
         var formattedDate = mm + '-' + dd + '-' + yyyy + '-' + hours + '-' + mins + '-' + seconds;
         return formattedDate;
     },
     
     getFormattedCurrentDate: function () {
         // January 5th, 2019  will be returned in 01-05-2019 format
         var today = new Date();
         var year = today.getFullYear();
         var month = (today.getMonth() + 1);
         var day = today.getDate();
         if (month < 10) month = '0' + month;
         if (day < 10) day = '0' + day;
         return month + '-' + day + '-' + year;
     },
     
     // Checks if the TIN has 9 digits//
     validateTIN: function (tin) {
         return /\b[a-zA-Z0-9]{9}\b/.test(tin);
     },
 
     // Checks if the NPI is 10 digit number or not //
     validateNPI: function(npi) {
         return /\b[a-zA-Z0-9]{10}\b/.test(npi);
     },
     
     // checks if modelType contains only alphabet //
     validateModelType: function(modelType){
         // var regex = /^[a-zA-Z ]*$/;
         return /^[a-zA-Z ]*$/.test(modelType);
     },

     // checks if dateInput is in mm/dd/yyyy format //
     validateDate: function(dateInput){
         return /^(0?[1-9]|1[0-2])\/(0?[1-9]|1\d|2\d|3[01])\/(19|20)\d{2}$/.test(dateInput);
     }
 }