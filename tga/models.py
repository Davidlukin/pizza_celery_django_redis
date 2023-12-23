from django.db import models

# Create your models here.

class Profile(models.Model):
    external_id = models.PositiveIntegerField(
        verbose_name= 'ID пользователя в социальной сети ',
    )
    name = models.TextField(
        verbose_name= 'Имя пользователя',
    )

    def __str__(self):
        return f'#{self.external_id} {self.name}'

    class Meta:
        verbose_name  = 'Профиль'

class Message(models.Model):
    profile = models.ForeignKey(
        to = 'tga.Profile',
        verbose_name='Профиль',
        on_delete= models.PROTECT,
    )
    text = models.TextField(
        verbose_name='Текст',
    )
    created_at = models.DateTimeField(
        verbose_name='Время получения',
        auto_now_add=True,
    )

    def __str__(self):
        return f'Сообщение{self.pk} от {self.profile}'

    class Meta:
        verbose_name = 'Сообщение'
        verbose_name_plural = 'Сообщения'