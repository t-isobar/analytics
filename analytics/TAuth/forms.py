from django import forms
from . import models


class SignInForm(forms.Form):
    login = forms.CharField(widget=forms.TextInput(attrs={"placeholder": "Enter your login"}), label='', required=True)
    password = forms.CharField(widget=forms.PasswordInput(attrs={'placeholder': "Enter your password"}), label='',
                               required=True)


class SignUpForm(forms.Form):
    email = forms.CharField(widget=forms.EmailInput(attrs={"placeholder": "yourname@yourmail.com"}), label="",
                            required=True)
    name = forms.CharField(widget=forms.TextInput(attrs={"placeholder": "Enter your first name"}), label="",
                           required=True)
    surname = forms.CharField(widget=forms.TextInput(attrs={"placeholder": "Enter your last name"}), label="",
                              required=True)
    password = forms.CharField(widget=forms.PasswordInput(attrs={"placeholder": "Enter your password"}), label='',
                               required=True)
    password2 = forms.CharField(widget=forms.PasswordInput(attrs={"placeholder": "Enter your password again"}),
                                label='', required=True)