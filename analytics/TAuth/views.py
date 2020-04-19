from django.http import HttpResponse, HttpResponseRedirect
from django.shortcuts import render, redirect
from .forms import SignInForm, SignUpForm


def start_page(request):
    return HttpResponse("Hello from start page!")


def sign_in(request):
    return render(request, 'i-auth/sign-in.html', context={"SignInForm": SignInForm})


def sign_up(request):
    return render(request, 'i-auth/sign-up.html', context={"SignUpForm": SignUpForm})
